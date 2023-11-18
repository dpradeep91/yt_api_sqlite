import streamlit as st
import Mongo_tasks as mt
import MySQL_tasks
import time
from streamlit import session_state as ss
import pandas as pd

st.markdown("# Transfer data to MySQL")

output = None

questionList = [
     'Select',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'    
]

if 'flag' not in ss:
    ss.flag = False

def changeSS(key):
    ss[key] = True


option = st.selectbox(
    'Please select SQL Question:',
     questionList ,
     on_change=changeSS,
     args =  ('flag',)
     )



def answers(option):
    
    
    idx = option.split('. ')[0]
    mydb, mycursor = MySQL_tasks.connection_string()
    if idx == '1':
        output = MySQL_tasks.mySQLResult(mydb, 'select v.video_name, c.channel_name from video v join playlist p on v.playlist_id = p.playlist_id join channel c on p.channel_id=c.channel_id')
    elif idx == '2':
        output = MySQL_tasks.mySQLResult(mydb, 'with temp as (select c.channel_id,c.channel_name,count(v.video_id) as no_of_videos from video v join playlist p on v.playlist_id = p.playlist_id join channel c on p.channel_id=c.channel_id group by c.channel_id,c.channel_name)select channel_id,channel_name, max(no_of_videos) as no_of_videos from temp GROUP by channel_id,channel_name')
    elif idx == '3':
        output = MySQL_tasks.mySQLResult(mydb, 'select c.channel_name,v.video_name,cast(v.view_count as text) view_count from video v join playlist p on v.playlist_id = p.playlist_id join channel c on p.channel_id=c.channel_id order by v.view_count desc limit 10')
    elif idx == '4':
        output = MySQL_tasks.mySQLResult(mydb, 'with temp as (select v.video_name,count(c.comment_id) no_of_comments from video v join comment c on v.video_id = c.video_id GROUP by v.video_name) select * from temp order by no_of_comments desc')
    elif idx == '5':
        output = MySQL_tasks.mySQLResult(mydb, 'with temp as(select c.channel_name,v.video_name,cast(v.like_count as text), rank() over (partition by c.channel_name order by v.like_count desc) as rnk from video v join playlist p on v.playlist_id = p.playlist_id join channel c on p.channel_id=c.channel_id)select channel_name,video_name,like_count from temp where rnk=1')
    elif idx == '6':
        output = MySQL_tasks.mySQLResult(mydb, 'with temp as (select v.video_name,cast(sum(v.like_count) as text) like_count,cast(sum(v.dislike_count) as text) as dislike_count from video v group by v.video_name) select * from temp order by like_count desc, dislike_count desc')
    elif idx == '7':
        output = MySQL_tasks.mySQLResult(mydb, 'with temp as (select c.channel_name,cast(sum(cast(c.channel_views as numeric)) as text) channel_views from channel c group by channel_name )  select * from temp order by channel_views desc')
    elif idx == '8':
        output = MySQL_tasks.mySQLResult(mydb, 'select distinct c.channel_name from video v join playlist p on v.playlist_id = p.playlist_id join channel c on p.channel_id=c.channel_id where extract(year from v.published_date)=2022')
    elif idx == '9':
        output = MySQL_tasks.mySQLResult(mydb, 'with temp as (select  c.channel_name,avg(v.duration) avg_duration_sec from video v join playlist p on v.playlist_id = p.playlist_id join channel c on p.channel_id=c.channel_id group by c.channel_name) select * from temp order by avg_duration_sec desc')
    elif idx == '10':
        output = MySQL_tasks.mySQLResult(mydb, 'with temp as(select c.channel_name,v.video_name,cast(v.comment_count as text) comment_count, rank() over (partition by c.channel_name order by v.comment_count desc) as rnk from video v join playlist p on v.playlist_id = p.playlist_id join channel c on p.channel_id=c.channel_id) select channel_name,video_name,comment_count from temp where rnk=1 order by channel_name')
    else:
        ss.flag = False
        output = None
    
    st.table(output)
    

if ss.flag:
    answers(option)
else:
    output = None






