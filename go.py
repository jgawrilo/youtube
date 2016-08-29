#!/usr/bin/env python

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.tools import argparser
import json
import os
import codecs
from bs4 import BeautifulSoup
import argparse
import requests

'''

Script to pull youtube.
More info here: https://developers.google.com/youtube/v3/docs/

Call with ./go.py --key <google_key> --playlistfile <path_to_playlist_file>

playlist file should look like (one id per line)

PLA292168BA44CB134
PLS3UB7jaIERzUiBvpSOHtHc_dzhV067jC



'''
def do_video_comments(pid,vid,comments,output):
    for cid in comments:
        if "topLevelComment" in comments[cid]["snippet"]:
            top = comments[cid]["snippet"]["topLevelComment"]["snippet"]
            ctype = "comment"
        else:
            top = comments[cid]["snippet"]
            ctype = "reply"
        
        authorChannel = top.get("authorChannelId",{"value":""})["value"]
        dt = top["publishedAt"]
        name = top["authorDisplayName"]
        text = BeautifulSoup(top["textDisplay"]).getText().replace("\t"," ").replace("\n"," ").replace("\r"," ").replace("\r\n"," ")
        output.write("\t".join((ctype,pid,authorChannel,cid,vid,dt,text,name)) + "\n")


def get_channels_from_comments(comments):

    channels = set()
    for comment_call in comments:
        for comment in comment_call.get("items", []):  
            if "authorChannelId" in comment["snippet"]["topLevelComment"]["snippet"]:
                channels.add(comment["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"])
            if "replies" in comment:
                for reply in comment["replies"]["comments"]:
                    if "authorChannelId" in reply["snippet"]:
                        channels.add(reply["snippet"]["authorChannelId"]["value"])

    return channels


def check_comments(comments):

    comment_ids_needed = []
    all_comment_ids = {}
    for comment_call in comments:
        for comment in comment_call.get("items", []):  
            all_comment_ids[comment["id"]] = comment
            total_snagged = 0
            total_replies = comment["snippet"]["totalReplyCount"]
            if "replies" in comment:
                total_snagged = len(comment["replies"]["comments"])
                for reply in comment["replies"]["comments"]:
                    all_comment_ids[reply["id"]] = reply

            if total_snagged != total_replies:
                comment_ids_needed.append(comment["id"])
    return comment_ids_needed, all_comment_ids

def get_channel_for_id(xid, youtube):
    response = youtube.channels().list(
    part="id,snippet,contentDetails",
    id=xid,
    maxResults=20
    ).execute()
    return response

def get_commentsThreads_for_video(vid, youtube):
    try:
        data = []
        response = youtube.commentThreads().list(
        part="id,snippet,replies",
        videoId=vid,
        maxResults=20
        ).execute()

        data.append(response)

        while "nextPageToken" in response:
            print "Pulling more comments..."
            response = youtube.commentThreads().list(
            part="id,snippet,replies",
            videoId=vid,
            pageToken=response["nextPageToken"],
            maxResults=20
            ).execute()
            data.append(response)

        return data

    except HttpError, e:
      print "An HTTP error %d occurred:\n%s" % (e.resp.status, e.content)
      return []

def get_comment(cid, youtube):
    data = []
    response = youtube.comments().list(
    part="id,snippet",
    parentId=cid,
    maxResults=20
    ).execute()

    data.append(response)

    while "nextPageToken" in response:
        print "Pulling more comments..."
        response = youtube.comments().list(
        part="id,snippet",
        parentId=cid,
        pageToken=response["nextPageToken"],
        maxResults=20
        ).execute()
        data.append(response)

    return data

def get_subscriptions_for_channel(cid, youtube):
    response = youtube.subscriptions().list(
    part="id,snippet,contentDetails",
    channelId=chan,
    maxResults=20
    ).execute()
    return response

def get_activities_for_channel(cid, youtube):
    data = []
    response = youtube.activities().list(
    part="id,snippet,contentDetails",
    channelId=cid,
    maxResults=20
    ).execute()

    data.append(response)

    while "nextPageToken" in response:
        print "Pulling more activities..."
        response = youtube.activities().list(
        part="id,snippet,contentDetails",
        channelId=cid,
        pageToken=response["nextPageToken"],
        maxResults=20
        ).execute()
        data.append(response)

    return data

def get_video_info(vid, youtube):
    response = youtube.videos().list(
    part="id,snippet,contentDetails,statistics",
    id=vid,
    maxResults=20
    ).execute()
    return response

def get_videos_from_playlist(pid, youtube):
    response = youtube.playlistItems().list(
    part="id,snippet,contentDetails,status",
    playlistId=pid,
    maxResults=20
    ).execute()
    return response

def get_playlist_info(pid, youtube):
    response = youtube.playlists().list(
    part="id,snippet,contentDetails,status,player,localizations",
    id=pid,
    maxResults=20
    ).execute()
    return response


# MAIN
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Pull some youtube.')

    parser.add_argument("--key", help="https://cloud.google.com/console")

    parser.add_argument("--playlistfile", help="file with playlists id's per line")

    args = parser.parse_args()

    # Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
    # tab of
    #   https://cloud.google.com/console
    # Please ensure that you have enabled the YouTube Data API for your project.

    DEVELOPER_KEY = args.key
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"


    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
        developerKey=DEVELOPER_KEY)

    playlist_info_dir = "./playlist_info/"

    playlist_dir = "./playlists/"

    video_dir = "./videos/"

    comments_dir = "./comments/"

    comment_ids_dir = "./comment_ids/"

    channels_dir = "./channels/"

    for d in [playlist_info_dir, playlist_dir, video_dir, comments_dir, comment_ids_dir, channels_dir]:
        if not os.path.exists(d):
            os.makedirs(d)


    comment_output = codecs.open("all_video_comments.txt", 'w', 'utf-8')
    comment_output.write("\t".join(("type","playlist_id","author","comment_id","video_id","datetime","text","name")) + "\n")
    
    playListIDs = open(args.playlistfile).readlines()

    # for all playlists
    playlist_count = 0
    video_count = 0
    channel_count = 0
    comment_count = 0

    for pid in playListIDs:
        print "Playlist:", pid
        if not os.path.isfile(playlist_info_dir + pid):
            print "Pulling Playlist data..."
            with codecs.open(playlist_info_dir + pid, 'w', 'utf-8') as output:
                pinfo_data = get_playlist_info(pid, youtube)
                output.write(json.dumps(pinfo_data,ensure_ascii=False, encoding='utf8',indent=2))
        else:
            with codecs.open(playlist_info_dir + pid, 'r', 'utf-8') as pinput:
                pinfo_data = json.loads(" ".join(pinput.readlines()))

    for pid in playListIDs:
        print "Playlist:", pid
        playlist_count += 1
        if not os.path.isfile(playlist_dir + pid):
            print "Pulling Playlist data..."
            with codecs.open(playlist_dir + pid, 'w', 'utf-8') as output:
                p_data = get_videos_from_playlist(pid, youtube)
                output.write(json.dumps(p_data,ensure_ascii=False, encoding='utf8',indent=2))
        else:
            with codecs.open(playlist_dir + pid, 'r', 'utf-8') as pinput:
                p_data = json.loads(" ".join(pinput.readlines()))

        # for all videos in playlist
        for video in p_data.get("items",[]):
            vid = video["contentDetails"]["videoId"]
            video_count += 1
            print "On video:", vid
            if not os.path.isfile(video_dir + vid):
                print "Pulling Video data..."
                with codecs.open(video_dir + vid, 'w', 'utf-8') as output:
                    v_data = get_video_info(vid, youtube)
                    output.write(json.dumps(v_data,ensure_ascii=False, encoding='utf8',indent=2))
            else:
                with codecs.open(video_dir + vid, 'r', 'utf-8') as vinput:
                    v_data = json.loads(" ".join(vinput.readlines()))

            #print v_data
            num_comments = 0

            if v_data.get("items"):
                num_comments = v_data.get("items",[{"statistics":{}}])[0]["statistics"].get("commentCount",0)

            if not os.path.isfile(comments_dir + vid):
                print "Pulling Comment data..."
                with codecs.open(comments_dir + vid, 'w', 'utf-8') as output:
                    c_data = get_commentsThreads_for_video(vid, youtube)
                    output.write(json.dumps(c_data,ensure_ascii=False, encoding='utf8',indent=2))
            else:
                with codecs.open(comments_dir + vid, 'r', 'utf-8') as cinput:
                    c_data = json.loads(" ".join(cinput.readlines()))

            comments, all_comment_ids = check_comments(c_data)

            channels = get_channels_from_comments(c_data)

            for comment in comments:
                if not os.path.isfile(comment_ids_dir + comment):
                    print "Pulling Comment data..."
                    with codecs.open(comment_ids_dir + comment, 'w', 'utf-8') as output:
                        cid_data = get_comment(comment, youtube)
                        output.write(json.dumps(cid_data,ensure_ascii=False, encoding='utf8',indent=2))
                else:
                    with codecs.open(comment_ids_dir + comment, 'r', 'utf-8') as cinput:
                        cid_data = json.loads(" ".join(cinput.readlines()))

                for comment_call in cid_data:
                    for comment_item in comment_call.get("items",[]):
                        all_comment_ids[comment_item["id"]] = comment_item
                        if "authorChannelId" in comment_item["snippet"]:
                            channels.add(comment_item["snippet"]["authorChannelId"]["value"])

            channel_count += len(channels)

            for channel in channels:
                if not os.path.isfile(channels_dir + channel):
                    print "Pulling Channel data..."
                    with codecs.open(channels_dir + channel, 'w', 'utf-8') as output:
                        channel_data = get_activities_for_channel(channel, youtube)
                        output.write(json.dumps(channel_data,ensure_ascii=False, encoding='utf8',indent=2))
                else:
                    with codecs.open(channels_dir + channel, 'r', 'utf-8') as cinput:
                        channel_data = json.loads(" ".join(cinput.readlines()))

            comment_count += len(all_comment_ids.keys())

            print comment_count, "total comments."

            if int(num_comments) != len(all_comment_ids.keys()):
                print "** Comment mismatch **", num_comments, len(all_comment_ids.keys()), " - no big deal though"

            
            do_video_comments(pid,vid,all_comment_ids,comment_output)
