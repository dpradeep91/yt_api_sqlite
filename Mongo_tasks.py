
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
import MySQL_tasks  # Custom Module
import demoji
import config as c
demoji.download_codes()


uri = "mongodb+srv://"+ c.mn_pwd + "@cluster0.qgxfiuy.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

def getAllInfo():
    ch_name = []
    db = client.YT_API #db
    records = db.yt_result_set  # Collections
    result = records.find({})
    for item in result:
        ch_name.append(demoji.replace(item['channel']['channel_name'].replace("'"," ").replace('\n',' '),''))
    return ch_name

def saveDetails(channel_name):
    try:
        
        db = client.YT_API #db
        records = db.yt_result_set  # Collections
        result = records.find_one({'channel.channel_name':channel_name},{'_id':False})
       
        mydb, mycursor = MySQL_tasks.connection_string()
        MySQL_tasks.insert_channel(mydb,
                                        mycursor,
                                        demoji.replace(result['channel']['channel_id'].replace("'"," ").replace('\n',' '),''),
                                        demoji.replace(channel_name.replace("'"," ").replace('\n',' '),''),                                        
                                        demoji.replace(result['channel']['channel_views'].replace("'"," ").replace('\n',' '),''),
                                        demoji.replace(result['channel']['channel_description'].replace("'"," ").replace('\n',' '),''),
                                        demoji.replace(result['channel']['channel_status'].replace("'"," ").replace('\n',' '),'')
                                        )
        
        for items in result['playlists']:
            MySQL_tasks.insert_playlist(mydb,
                                                mycursor,
                                                demoji.replace(items['playlist_id'].replace("'"," ").replace('\n',' '),''),
                                                demoji.replace(result['channel']['channel_id'].replace("'"," ").replace('\n',' '),''),
                                                demoji.replace(items['playlist_name'].replace("'"," ").replace('\n',' '),'')
                                                )
        
        for items in result['video']:
            MySQL_tasks.insert_video(mydb,
                                            mycursor,
                                            demoji.replace(items['Video_Id'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['playlist_id'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['Video_Name'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['Video_Description'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['published_date'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['view_count'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['like_count'].replace("'"," ").replace('\n',' '),''),
                                            '0',
                                            demoji.replace(items['favorite_count'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['comment_count'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(str(items['duration']).replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['thumbnail'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(items['caption_status'].replace("'"," ").replace('\n',' '),'')
                                            )
            
        
            
            for item in items['Comments']:
                MySQL_tasks.insert_comment(mydb,
                                            mycursor,
                                            demoji.replace(item['Comment_Id'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(item['Video_Id'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(item['Comment_Text'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(item['Comment_Author'].replace("'"," ").replace('\n',' '),''),
                                            demoji.replace(item['Comment_Published_At'].replace("'"," ").replace('\n',' '),'')
                                            )
        
            
    except Exception as e:
        print(e)