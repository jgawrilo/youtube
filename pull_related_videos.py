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
import  googleapiclient

def get_video_info(vid, youtube):
    response = youtube.videos().list(
    part="id,snippet,contentDetails,statistics",
    id=vid,
    maxResults=1
    ).execute()
    return response


def get_video_suggestions(youtube,vid):
    try:
        #print "Related to:", vid
        search_response = youtube.search().list(
        type="video",
        part="id",
        relatedToVideoId=vid,
        maxResults=20
        ).execute()
        for i in search_response["items"]:
            #print float(get_video_info(i["id"]["videoId"],youtube)["items"][0]["statistics"]["viewCount"])
            if float(get_video_info(i["id"]["videoId"],youtube)["items"][0]["statistics"]["viewCount"]) < 100000:
                print i["id"]["videoId"]
    except googleapiclient.errors.HttpError:
        pass


# MAIN
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Pull some youtube.')

    parser.add_argument("--key", help="https://cloud.google.com/console")

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

    for f in os.listdir("../flashpoint/videos/"):
        get_video_suggestions(youtube,f)