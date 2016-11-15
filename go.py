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
import sys

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
        text = BeautifulSoup(top["textDisplay"],"lxml").getText().replace("\t"," ").replace("\n"," ").replace("\r"," ").replace("\r\n"," ")
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
            print len(data)
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
        print len(data)
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
        print len(data)
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

def get_video_suggestions(youtube):
    search_response = youtube.search().list(
    type="video",
    part="id",
    relatedToVideoId="xXgeoFlUY8Y",
    maxResults=20
    ).execute()
    print json.dumps(search_response,indent=2)


# MAIN
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Pull some youtube.')

    parser.add_argument("--key", help="https://cloud.google.com/console", required=True)

    parser.add_argument("--playlistfile", help="file with playlists id's per line", required=True)

    parser.add_argument("--name", help="name of pull", required=True)

    args = parser.parse_args()

    DATA_DIR = "./data/" + args.name + "/"

    # Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
    # tab of
    #   https://cloud.google.com/console
    # Please ensure that you have enabled the YouTube Data API for your project.

    DEVELOPER_KEY = args.key
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"


    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
        developerKey=DEVELOPER_KEY)

    #get_video_suggestions(youtube)
    #sys.exit()

    playlist_info_dir = DATA_DIR + "playlist_info/"

    playlist_dir = DATA_DIR + "playlists/"

    video_dir = DATA_DIR + "videos/"

    comments_dir = DATA_DIR + "comments/"

    comment_ids_dir = DATA_DIR + "comment_ids/"

    channels_dir = DATA_DIR + "channels/"

    for d in [DATA_DIR, playlist_info_dir, playlist_dir, video_dir, comments_dir, comment_ids_dir, channels_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    comment_output = codecs.open(DATA_DIR + "all_video_comments.txt", 'w', 'utf-8')
    comment_output.write("\t".join(("type","playlist_id","author","comment_id","video_id","datetime","text","name")) + "\n")
    
    playListIDs = open(args.playlistfile).readlines()

    # for all playlists
    playlist_count = 0

    pids = []

    for pid in playListIDs:
        pid = pid.strip()
        playlist_count += 1
        pids.append(pid)
        print "Playlist:", pid
        if not os.path.isfile(playlist_info_dir + pid):
            print "Pulling Playlist data..."
            with codecs.open(playlist_info_dir + pid, 'w', 'utf-8') as output:
                pinfo_data = get_playlist_info(pid, youtube)
                output.write(json.dumps(pinfo_data,ensure_ascii=False, encoding='utf8',indent=2))
        else:
            print "Already pulled playlist data..."
            with codecs.open(playlist_info_dir + pid, 'r', 'utf-8') as pinput:
                pinfo_data = json.loads(" ".join(pinput.readlines()))

    print playlist_count, "total playlists."
    
    for pid in pids:
        print "On Playlist:", pid
        
        if not os.path.isfile(playlist_dir + pid):
            print "Pulling Videos for playlist:" + pid
            with codecs.open(playlist_dir + pid, 'w', 'utf-8') as output:
                p_data = get_videos_from_playlist(pid, youtube)
                output.write(json.dumps(p_data,ensure_ascii=False, encoding='utf8',indent=2))
        else:
            print "Already pulled videos for playlist..."
            with codecs.open(playlist_dir + pid, 'r', 'utf-8') as pinput:
                p_data = json.loads(" ".join(pinput.readlines()))

        # for all videos in playlist
        print len(p_data.get("items",[])), "total videos."
        for video in p_data.get("items",[]):
            channel_set = set()
            comment_set = set()
            num_activities = 0
            vid = video["contentDetails"]["videoId"]
            print "On video:", vid
            if not os.path.isfile(video_dir + vid):
                print "Pulling Video data..."
                with codecs.open(video_dir + vid, 'w', 'utf-8') as output:
                    v_data = get_video_info(vid, youtube)
                    output.write(json.dumps(v_data,ensure_ascii=False, encoding='utf8',indent=2))
            else:
                print "Already pulled data for video..."
                with codecs.open(video_dir + vid, 'r', 'utf-8') as vinput:
                    v_data = json.loads(" ".join(vinput.readlines()))

            #print v_data
            num_comments = 0

            if v_data.get("items"):
                num_comments = v_data.get("items",[{"statistics":{}}])[0]["statistics"].get("commentCount",0)

            print "Pulling Comment Threads data..."

            if not os.path.isfile(comments_dir + vid):
                with codecs.open(comments_dir + vid, 'w', 'utf-8') as output:
                    c_data = get_commentsThreads_for_video(vid, youtube)
                    output.write(json.dumps(c_data,ensure_ascii=False, encoding='utf8',indent=2))
            else:
                with codecs.open(comments_dir + vid, 'r', 'utf-8') as cinput:
                    c_data = json.loads(" ".join(cinput.readlines()))

            comments, all_comment_ids = check_comments(c_data)

            channels = get_channels_from_comments(c_data)

            print "Pulling Comment data..."
            for comment in comments:
                if not os.path.isfile(comment_ids_dir + comment):
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

            channel_set.update(channels)

            print "Pulling Channel data..."
            for channel in channels:
                if not os.path.isfile(channels_dir + channel):
                    with codecs.open(channels_dir + channel, 'w', 'utf-8') as output:
                        channel_data = get_activities_for_channel(channel, youtube)
                        output.write(json.dumps(channel_data,ensure_ascii=False, encoding='utf8',indent=2))
                else:
                    with codecs.open(channels_dir + channel, 'r', 'utf-8') as cinput:
                        channel_data = json.loads(" ".join(cinput.readlines()))
                        num_activities = len(channel_data)

            comment_set.update(all_comment_ids.keys())

            
            print len(channel_set), "total channels."
            print len(comment_set), "total comments."
            print num_activities, "total activities."

            print num_comments, "comments in stats."

            do_video_comments(pid,vid,all_comment_ids,comment_output)
