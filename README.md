from googleapiclient.discovery  import build
import pymongo
import MySQLdb
import mysql.connector as sql
from mysql.connector import Error
import pandas as pd
import pymysql
import streamlit as st
import re


def Api_connect():
    Api_Id="AIzaSyDXMN2Dr1SMa5X6Az951nkjo1p2d6sQBEk"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()


#get channel information
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    

#madhan gowri     :"UCY6KjrDBN_tIRFT_QNqQbRQ"
#un signed        :"UCXnDDUQyJpRfC98_ZRIuhZA"
#naked science    :"UC8JT2m0mKEgvEtie3JNKwew"

#get playlist ids
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data


#get video ids
def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                    part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
            res = youtube.playlistItems().list( 
                                            part = 'snippet',
                                            playlistId = playlist_id, 
                                            maxResults = 50,
                                            pageToken = next_page_token).execute()
            
            for i in range(len(res['items'])):
                video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = res.get('nextPageToken')
            
            if next_page_token is None:
                break
    return video_ids 


#get video information
def get_video_info(video_ids):
        video_data = []
        for video_id in video_ids:
                request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id= video_id)
                response = request.execute()

                for item in response["items"]:
                                data = dict(Channel_Name = item['snippet']['channelTitle'],
                                        Channel_Id = item['snippet']['channelId'],
                                        Video_Id = item['id'],
                                        Title = item['snippet']['title'],
                                        Tags = item['snippet'].get('tags'),
                                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                                        Description = item['snippet']['description'],
                                        Published_Date = item['snippet']['publishedAt'],
                                        Duration = item['contentDetails']['duration'],
                                        Views = item['statistics']['viewCount'],
                                        Likes = item['statistics'].get('likeCount'),
                                        Comments = item['statistics'].get('commentCount'),
                                        Favorite_Count = item['statistics']['favoriteCount'],
                                        Definition = item['contentDetails']['definition'],
                                        Caption_Status = item['contentDetails']['caption']
                                        )
                                video_data.append(data)

        return video_data


#get comment information
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information
        


#MongoDB Connection
client = pymongo.MongoClient("mongodb+srv://sureshselvaraj418:sureshsel@cluster0.dh97cht.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_data"]


# upload to MongoDB

def channel_details(channel_id):
        ch_details = get_channel_info(channel_id)
        pl_details = get_playlist_info(channel_id)
        video_ids = get_channel_videos(channel_id)
        vi_details = get_video_info(video_ids)
        com_details = get_comment_info(video_ids)

        coll1 = db["channel_details"]
        coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                        "comment_information":com_details})
        
        return "upload completed successfully"


# CONNECTING WITH MYSQL DATABASE
def channels_table():
        mydb = sql.connect(host="localhost",
                                                user="root",
                                                password="$Rakshan02@",
                                                database= "youtube_data",
                                                port = "3306"
                                                
                                                )
        cursor = mydb.cursor()
        drop_query = "DROP TABLE IF EXISTS channels"
        cursor.execute(drop_query)
        mydb.commit()
        try:
                        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(100) primary key, 
                                                                Subscription_Count bigint, 
                                                                Views bigint,
                                                                Total_Videos int,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(100))'''
                        cursor.execute(create_query)
                        mydb.commit()
        except:
                                print('channel already there')

        ch_list = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                        ch_list.append(ch_data["channel_information"])
        df = pd.DataFrame(ch_list)

        for index,row in df.iterrows():
                                insert_query = '''INSERT into channels(Channel_Name,
                                                                        Channel_Id,
                                                                        Subscription_Count,
                                                                        Views,
                                                                        Total_Videos,
                                                                        Channel_Description,
                                                                        Playlist_Id)
                                                                VALUES(%s,%s,%s,%s,%s,%s,%s)'''
                                

                                values =(
                                        row['Channel_Name'],
                                        row['Channel_Id'],
                                        row['Subscription_Count'],
                                        row['Views'],
                                        row['Total_Videos'],
                                        row['Channel_Description'],
                                        row['Playlist_Id'])
                                try:                     
                                     cursor.execute(insert_query,values)
                                     mydb.commit()    
                                except:
                                 print("Channels values are already inserted")



def playlists_table():
    mydb = sql.connect(host="localhost",
                                                    user="root",
                                                    password="$Rakshan02@",
                                                    database= "youtube_data",
                                                    port = "3306"
                                                    
                                                    )
    cursor = mydb.cursor()
    drop_query = "DROP TABLE IF EXISTS playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
            create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                            Title varchar(80), 
                            ChannelId varchar(100), 
                            ChannelName varchar(100),
                            PublishedAt timestamp,
                            VideoCount int
                            )'''
            cursor.execute(create_query)
            mydb.commit()
    except:
            print("Playlists Table alredy created")  

    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)
    df["PublishedAt"]=df["PublishedAt"].str.replace("T", " ").str.replace("Z", " ")

    for index,row in df.iterrows():
            insert_query = '''INSERT into playlists(PlaylistId,
                                                        Title,
                                                        ChannelId,
                                                        ChannelName,
                                                        PublishedAt,
                                                        VideoCount)
                                            VALUES(%s,%s,%s,%s,%s,%s)'''            
            values =(
                    row['PlaylistId'],
                    row['Title'],
                    row['ChannelId'],
                    row['ChannelName'],
                    row['PublishedAt'],
                    row['VideoCount'])
                    
            try:                     
              cursor.execute(insert_query,values)
              mydb.commit()    
            except:
              print("Playlists values are already inserted")



def videos_table():
    mydb = sql.connect(host="localhost",
                                                        user="root",
                                                        password="$Rakshan02@",
                                                        database= "youtube_data",
                                                        port = "3306"
                                                        
                                                        )
    cursor = mydb.cursor()
    drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(
                                Channel_Name varchar(150),
                                Channel_Id varchar(100),
                                Video_Id varchar(50) primary key, 
                                Title varchar(150), 
                                Tags text,
                                Thumbnail varchar(225),
                                Description text, 
                                Published_Date datetime,
                                Duration time, 
                                Views bigint, 
                                Likes bigint,
                                Comments int,
                                Favorite_Count int, 
                                Definition varchar(10), 
                                Caption_Status varchar(50) 
                                )''' 
                                
        cursor.execute(create_query)             
        mydb.commit()
    except:
        print("Videos Table alrady created")

    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
    df2["Published_Date"]=df2["Published_Date"].str.replace("T", " ").str.replace("Z", " ")
    df3=df2['Tags'][0]
    listToStr=' , '.join([str(elem) for elem in df3])
    df2["Tags"]=listToStr


    for index, row in df2.iterrows():
                insert_query = '''
                            INSERT INTO videos (Channel_Name,
                                Channel_Id,
                                Video_Id, 
                                Title, 
                                Tags,
                                Thumbnail,
                                Description, 
                                Published_Date,
                                Duration, 
                                Views, 
                                Likes,
                                Comments,
                                Favorite_Count, 
                                Definition, 
                                Caption_Status 
                                )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                        '''
                
                duration=row['Duration']    
                pattern = r'PT(?:(\d+)M)?(?:(\d+)S)?'
                match = re.match(pattern, duration)
                if match:
                    minutes = int(match.group(1) or 0)
                    seconds = int(match.group(2) or 0)
                total_seconds = minutes * 60 + seconds
                formatted_duration = '{:02d}:{:02d}:00'.format(total_seconds // 3600, (total_seconds % 3600) // 60)
                values = (
                            row['Channel_Name'],
                            row['Channel_Id'],
                            row['Video_Id'],
                            row['Title'],
                            row['Tags'],
                            row['Thumbnail'],
                            row['Description'],
                            row['Published_Date'],
                            formatted_duration,
                            row['Views'],
                            row['Likes'],
                            row['Comments'],
                            row['Favorite_Count'],
                            row['Definition'],
                            row['Caption_Status'])
                                        
                try:    
                    cursor.execute(insert_query,values)
                    mydb.commit()
                except:
                    print("videos values already inserted in the table")
            



def comments_table():
    mydb = sql.connect(host="localhost",
                                                            user="root",
                                                            password="$Rakshan02@",
                                                            database= "youtube_data",
                                                            port = "3306"
                                                            
                                                        )
    cursor = mydb.cursor()
    drop_query = "DROP TABLE IF EXISTS comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
            create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                                Video_Id varchar(80),
                                Comment_Text text, 
                                Comment_Author varchar(150),
                                Comment_Published datetime)'''
            cursor.execute(create_query)
            mydb.commit()
    except:
            print("Commentsp Table already created")


    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)
    df3["Comment_Published"]=df3["Comment_Published"].str.replace("T", " ").str.replace("Z", " ")


    for index, row in df3.iterrows():
                insert_query = '''
                    INSERT INTO comments (Comment_Id,
                                        Video_Id ,
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)
                    VALUES (%s, %s, %s, %s, %s)

                '''
                values = (
                    row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                )
                try:
                     cursor.execute(insert_query,values)
                     mydb.commit()
                except:
                     print("This comments are already exist in comments table")



def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"


def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                    ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df


def show_playlists_table():
    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
                
    df1 = st.dataframe(pl_list)
    return df1

def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
#df3["Comment_Published"]=df3["Comment_Published"].str.replace("T", " ").str.replace("Z", " ")
    return df3



with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")


channel_id = st.text_input("Enter the Channel id")


if st.button("Collect and Store data"):
    #for channel in channels:
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("Channel details of the given channel details  already exists")
    else:
        output = channel_details(channel_id)
        st.success(output)


if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()



#SQL connection
mydb = sql.connect(host="localhost",
                                                    user="root",
                                                    password="$Rakshan02@",
                                                    database= "youtube_data",
                                                    port = "3306"
                                                    
                                                    )
cursor = mydb.cursor()


question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

if question == '1. All the videos and the Channel Name':
    query1 = '''select title as videos, channel_name as channelname from videos;'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    mydb.commit()
    df=pd.DataFrame(t1, columns=["video title","channel name"])
    st.write(df)

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
   
    t2=cursor.fetchall()
    mydb.commit()
    df2=pd.DataFrame(t2, columns=["Channel Name","No Of Videos"])
    st.write(df2)   

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)

    t3 = cursor.fetchall()
    mydb.commit()
    df3=pd.DataFrame(t3, columns = ["views","channel Name","video title"])
    st.write(df3)    

elif question == '4. Comments in each video':

    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    t4=cursor.fetchall()
    mydb.commit()
    df4=pd.DataFrame(t4, columns=["No Of Comments", "Video Title"])
    st.write(df4)

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                        where Likes is not null order by Likes desc;'''
    cursor.execute(query5)

    t5 = cursor.fetchall()
    mydb.commit()
    df5=pd.DataFrame(t5, columns=["video Title","channel Name","like count"])
    st.write(df5)   

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)

    t6 = cursor.fetchall()
    mydb.commit()
    df6=pd.DataFrame(t6, columns=["like count","video title"])
    st.write(df6)

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)

    t7=cursor.fetchall()
    mydb.commit()
    df7=pd.DataFrame(t7, columns=["channel name","total views"])
    st.write(df7)


elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)

    t8=cursor.fetchall()
    mydb.commit()
    df8=pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"])
    st.write(df8)

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName,SEC_TO_TIME(AVG(TIME_TO_SEC(Duration))) AS average_duration from videos group by Channel_Name;"
                                    
    cursor.execute(query9)

    t9=cursor.fetchall()
    mydb.commit()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    d1=pd.DataFrame(T9)
    st.write(d1)

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                    where Comments is not null order by Comments desc;'''
    cursor.execute(query10)

    t10=cursor.fetchall()
    mydb.commit()
    df10=pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
    st.write(df10)
       



