

import os
import config as c
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from googleapiclient.discovery import build
import json
import isodate
import demoji
demoji.download_codes()


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def apiValidation():
    api_key = c.api_key
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
    return youtube


youtube = apiValidation()

def getChannelInfo(channel_id):
    
    
    response = youtube.channels().list(
        id=channel_id,
        part='snippet,status,statistics,contentDetails'
    )

    channel_data = response.execute()
    return channel_data


scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

uri = "mongodb+srv://"+ c.mn_pwd + "@cluster0.qgxfiuy.mongodb.net/?retryWrites=true&w=majority"



def pymongo_connection():
# Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    
    try:
        db = client.YT_API #db
        records = db.yt_result_set  # Collections
        return records
    except Exception as e:
        print(e)

def pymongo_insert(items):
        records = pymongo_connection()
        records.insert_one(items)

def pymongo_delete(channel_nme):
    records = pymongo_connection()
    k = records.delete_one({'channel.channel_name':channel_nme})


def saveChannelInfo(channel_id):

    channel_data = getChannelInfo(channel_id)

    channel_informations = {
        'channel_id': channel_id,
        'channel_name' : channel_data['items'][0]['snippet']['title'],
        'channel_description' : channel_data['items'][0]['snippet']['description'],
        'playlists' : channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'], 
        'subscription_count' : channel_data['items'][0]['statistics']['subscriberCount'],
        'channel_views' : channel_data['items'][0]['statistics']['viewCount'],
        'channel_status' : channel_data['items'][0]['status']['privacyStatus']
    } 

    
    playlist_data = youtube.playlists().list(
        part='snippet',
        channelId=channel_id
    ).execute()

    playlist_name = playlist_data['items'][0]['snippet']['title']
    playlists = []

    for playlist in playlist_data['items']:
        playlist_id = playlist['id']
        playlist_name = playlist['snippet']['title']
        playlists.append({'playlist_id': playlist_id, 'playlist_name': playlist_name})

    #Retrieve the video IDs from the playlist
    videos = []
    combineData = []
    video_information = []
    next_page_token = None
    # Retrieve the playlist items
    while True:
        for playlist in playlists:
            playlist_items = youtube.playlistItems().list(
                playlistId=playlist['playlist_id'],
                part='snippet',
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            
            
            # Retrieve video information for each video in the playlist
            for item in playlist_items['items']:
                video_id = item['snippet']['resourceId']['videoId']
                video_response = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=video_id
                ).execute()
                

                if video_response['items']:
                        # Retrieve comments for the video
                    comment_threads = youtube.commentThreads().list(
                        part='snippet',
                        videoId=video_id,
                        maxResults=1000
                    ).execute()



                    comments = []
                    for comment_thread in comment_threads['items']:
                        comment_id = comment_thread['id']
                        comment_text = comment_thread['snippet']['topLevelComment']['snippet']['textDisplay']
                        comment_author = comment_thread['snippet']['topLevelComment']['snippet']['authorDisplayName']
                        comment_published_at = comment_thread['snippet']['topLevelComment']['snippet']['publishedAt']
                        comments.append({
                            "Video_Id": video_id,
                            "Comment_Id": comment_id,
                            "Comment_Text": comment_text,
                            "Comment_Author": comment_author,
                            "Comment_Published_At": comment_published_at
                        })

                
                for video_r in video_response['items']:
                    video_information.append({
                                "Video_Id": video_id,
                                "playlist_id": playlist['playlist_id'],
                                "Video_Name": video_r['snippet']['title'] if 'title' in video_r['snippet'] else "Not Available",
                                "Video_Description": video_r['snippet']['description'],
                                "published_date" : video_r['snippet']['publishedAt'],
                                "view_count" : video_r['statistics']['viewCount'],
                                "like_count": video_r['statistics']['likeCount'],
                                "favorite_count" : video_r['statistics']['favoriteCount'],
                                "comment_count" : video_r['statistics']['commentCount'],
                                "duration" : isodate.parse_duration(video_r['contentDetails']['duration']).total_seconds(),
                                "thumbnail" : video_r['snippet']['thumbnails']['default']['url'],
                                "caption_status" : video_r['contentDetails']['caption'],
                                "Comments": comments
                            })
                    
        combineData.append({
                        'channel':channel_informations,
                        'playlists': playlists,
                        'video': video_information
                    })  
        
        records = pymongo_connection()

        channel_nme = getChannelInfo(channel_id)['items'][0]['snippet']['title']  #Get channel name
        result = records.find_one({"channel.channel_name":channel_nme},{'_id':False})
        if result is not None:
            if demoji.replace(result['channel']['channel_name'].replace("'"," ").replace('\n',' '),'') == channel_nme:  # Checking if data exist in MongoDB
                pymongo_delete(channel_nme)
                print('Deleted')
            
        for item in combineData:  
            pymongo_insert(item)
        print('Inserted')
        break
       


    

    

    