import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd

api_key = "AIzaSyCJEoPq855NYESIwiAT7rqFcNy7lOkRYa8"
you_tube = build("youtube", "v3", developerKey=api_key)

client = pymongo.MongoClient("mongodb://iswarya:blue12345@ac-jgnafnu-shard-00-00.btxtni3.mongodb.net:27017,"
                             "ac-jgnafnu-shard-00-01.btxtni3.mongodb.net:27017,"
                             "ac-jgnafnu-shard-00-02.btxtni3.mongodb.net:27017/?ssl=true&replicaSet=atlas-1op5j8"
                             "-shard-0&authSource=admin&retryWrites=true&w=majority")
db = client["project_youtube"]  # creating database in MongoDB
channel_list = db.list_collection_names()  # Gives all the collections available in db

conn = psycopg2.connect(host="localhost", user="postgres", password="Smile!098", port=5432, database="youtube")
cur = conn.cursor()


def channel(c_id):  # Returns scrapped channel info
    channel_data = you_tube.channels().list(
        part="snippet, contentDetails, statistics",
        id=c_id).execute()

    channel_info = dict(channel_id=c_id,
                        channel_name=channel_data["items"][0]["snippet"]["title"],
                        subscriber_count=channel_data["items"][0]["statistics"]["subscriberCount"],
                        channel_view_count=channel_data["items"][0]["statistics"]["viewCount"],
                        channel_des=channel_data["items"][0]["snippet"]["description"])

    return channel_info


def playlist(c_id):  # Returns scrapped playlist info
    request = you_tube.playlists().list(
        part="snippet,contentDetails",
        channelId=c_id,
        maxResults=25)
    response = request.execute()

    playlist_info = []
    for i in response["items"]:
        playlist_data = dict(channel_id=c_id, playlist_id=i["id"], playlist_title=i["snippet"]["localized"]["title"])
        playlist_info.append(playlist_data)

    return playlist_info


def video_1():  # Returns list of video_ids in list
    playlist_stats_1 = playlist(channel_id)
    final_video_list = []
    for i in playlist_stats_1:
        one_playlist_id = i["playlist_id"]
        request = you_tube.playlistItems().list(
            part="contentDetails",
            playlistId=one_playlist_id,
            maxResults=50)
        response = request.execute()

        video_ids = []
        for j in range(len(response["items"])):
            video_ids.append(response["items"][j]["contentDetails"]["videoId"])

        next_page_token = response.get("nextPageToken")
        next_page = True

        while next_page:
            if next_page_token is None:
                next_page = False
            else:
                request = you_tube.playlistItems().list(
                    part="contentDetails",
                    playlistId=one_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token)
                response = request.execute()

                for k in range(len(response["items"])):
                    video_ids.append(response["items"][k]["contentDetails"]["videoId"])

                next_page_token = response.get("nextPageToken")

        final_video_list.append(video_ids)
    return final_video_list


def video_2(c_id):  # Returns scrapped video_info
    video_ids_list = video_1()
    video_info = []
    for sublist in video_ids_list:
        for element in sublist:
            exp_ele = element
            request = you_tube.videos().list(
                part="snippet,contentDetails,statistics",
                id=exp_ele)
            response = request.execute()
            for video in response["items"]:
                video_a = dict(channel_id=c_id, video_id=video["id"],
                               video_title=video["snippet"]["title"],
                               video_description=video["snippet"]["description"],
                               video_duration=video["contentDetails"]["duration"],
                               video_caption=video["contentDetails"].get('caption', 'false'),
                               view_count=video["statistics"]["viewCount"],
                               like_count=video["statistics"]["likeCount"],
                               dislike_count=video["statistics"].get("dislikeCount", 0),
                               favorite_count=video["statistics"].get("favoriteCount", 0),
                               comment_count=video["statistics"].get("comment_count", 0),
                               published_at=video["snippet"]["publishedAt"])
                video_info.append(video_a)
    return video_info


def video_comments():  # Returns comment info
    video_id = video_2(channel_id)
    comment_info = []
    for ele in video_id:
        exp_ele = ele["video_id"]
        next_page_token_comment = None
        request = you_tube.commentThreads().list(
            part="snippet",
            videoId=exp_ele,
            maxResults=50,
            pageToken=next_page_token_comment)
        response = request.execute()
        #  print(response)
        while True:
            if response["items"]:
                for j in response["items"]:
                    comment_1 = dict(video_id=j["snippet"]["topLevelComment"]["snippet"]["videoId"], comment_id=j["id"],
                                     published_at=j['snippet']['topLevelComment']['snippet']['publishedAt'],
                                     author_display_name=j['snippet']['topLevelComment']['snippet'][
                                         'authorDisplayName'],
                                     display_text=j['snippet']['topLevelComment']['snippet']['textDisplay'])
                    comment_info.append(comment_1)
            next_page_token_comment = response.get(next_page_token_comment)
            if next_page_token_comment is None:
                break
    return comment_info


def youtube_stats():
    channel_stats = channel(channel_id)
    playlist_stats = playlist(channel_id)
    video_1()
    video_data = video_2(channel_id)
    comments_data = video_comments()
    youtube = dict(channel_info=channel_stats, playlist_info=playlist_stats, video_info=video_data,
                   comment_info=comments_data)
    return youtube


st.set_page_config(layout="wide")
with st.sidebar:
    opt = option_menu(
        menu_title="Menu",
        options=["About", "Accumulation", "Migration", "Insights"],
        icons=["house", "braces", "table", "question"],
        menu_icon="cast",
        default_index=0
    )
if opt == "About":
    st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    st.write("The YouTube Data Scraper and Analysis web page is a powerful tool designed to provide valuable "
             "insights into YouTube channels based on user-provided channel IDs. By utilizing the YouTube API, "
             "the application extracts crucial details such as the channel name, subscriber count, like count, "
             "total videos, and comprehensive statistics for each video, including view count, likes, dislikes, "
             "and comments.")
    st.write("The webpage goes beyond data extraction and offers a range of analysis and visualization options "
             "using the collected data. Users can delve into the information to gain valuable insights into a "
             "channel's performance, audience engagement, and overall growth trajectory. The flexibility of Streamlit "
             "enables customization and expansion of the application to cater to specific analysis requirements, "
             "allowing users to uncover meaningful patterns and make data-driven decisions")
    st.write("For businesses, the YouTube Data Scraper and Analysis web application serves as a robust solution to "
             "extract, store, and analyze YouTube channel data. By harnessing the power of the scraped data, "
             "businesses can gain actionable insights, optimize content strategies, and make informed decisions to "
             "drive success on the platform. With its user-friendly features and the capability to perform sentiment "
             "analysis using machine learning techniques, the application provides a comprehensive toolkit for "
             "businesses to thrive and effectively connect with their audience on YouTube.")
    st.write("In summary, the YouTube Data Scraper and Analysis web application streamlines the process of gathering "
             "and analyzing data from YouTube channels. By leveraging the YouTube API, MongoDB, and MySQL, "
             "the application empowers users with a powerful toolset to extract valuable insights and facilitate "
             "data-driven decision-making in the dynamic landscape of YouTube content creation and consumption.")

elif opt == "Accumulation":
    st.title("YouTube Data Harvesting and Warehousing with MongoDB, SQL")
    col1, col2 = st.columns([6, 2])
    with col1:
        channel_id = st.text_input("Enter the channel id")
        page_names = ["Enter", "Preview", "Upload"]
        page = st.radio("select", page_names, horizontal=True, label_visibility="hidden")

        if channel_id and page == "Enter":
            st.write("You can now preview the json file or upload the file in MongoDB")

        if page == "Preview":
            preview_file = youtube_stats()
            st.write(preview_file)

        if page == "Upload":
            preview_file = youtube_stats()
            channel_name = preview_file["channel_info"]["channel_name"]
            col = db[channel_name]
            col.insert_one(preview_file)

elif opt == "Migration":
    st.title("YouTube Data Harvesting and Warehousing with MongoDB, SQL")  # Have to get the list channel_name here
    channel_id = st.text_input("Enter the channel id")
    id_selected = st.selectbox("Select a channel name to migrate the data from mongodb to sql",
                               options=channel_list)
    migrate = st.button("Migrate")
    if migrate:
        preview_file = youtube_stats()
        #  channel_id = preview_file["channel_info"]["channel_id"]  # Getting channel id from mongodb for migrating
        channel_dict = channel(channel_id)
        channel_df = pd.DataFrame([channel_dict])
        channel_df["subscriber_count"] = channel_df["subscriber_count"].astype("int64")
        channel_df["channel_view_count"] = channel_df["channel_view_count"].astype("int64")
        # print(channel_df)
        cur.execute(
            "CREATE TABLE IF NOT EXISTS CHANNEL(CHANNEL_ID VARCHAR(50) PRIMARY KEY, CHANNEL_NAME VARCHAR(75), "
            "SUBSCRIBER_COUNT INT, CHANNEL_VIEW_COUNT INT, CHANNEL_DESCRIPTION TEXT)")
        conn.commit()
        a = "INSERT INTO CHANNEL(CHANNEL_ID, CHANNEL_NAME, SUBSCRIBER_COUNT, CHANNEL_VIEW_COUNT, CHANNEL_DESCRIPTION) " \
            "VALUES(%s, %s, %s, %s, %s)"
        for index, value in channel_df.iterrows():
            val = value["channel_id"]
            cur.execute(f"SELECT * FROM CHANNEL WHERE CHANNEL_ID = '{val}'")
            w = cur.fetchall()
            if len(w) > 0:
                cur.execute(f"DELETE FROM CHANNEL WHERE CHANNEL_ID = '{val}'")
                conn.commit()
            result_1 = (
                value["channel_id"], value["channel_name"], value["subscriber_count"], value["channel_view_count"],
                value["channel_des"])
            cur.execute(a, result_1)
        conn.commit()

        playlist_dict = playlist(channel_id)
        playlist_df = pd.DataFrame(playlist_dict)
        # print(playlist_df.head)
        cur.execute("CREATE TABLE IF NOT EXISTS PLAYLIST(CHANNEL_ID VARCHAR(50), PLAYLIST_ID VARCHAR(75) PRIMARY KEY, "
                    "PLAYLIST_TITLE VARCHAR(100))")
        conn.commit()
        b = "INSERT INTO PLAYLIST(CHANNEL_ID, PLAYLIST_ID, PLAYLIST_TITLE) VALUES(%s, %s, %s)"
        for index, value_1 in playlist_df.iterrows():
            val_1 = value_1["playlist_id"]
            cur.execute(f"SELECT * FROM PLAYLIST WHERE PLAYLIST_ID = '{val_1}'")
            x = cur.fetchall()
            if len(x) > 0:
                cur.execute(f"DELETE FROM PLAYLIST WHERE PLAYLIST_ID = '{val_1}'")
                conn.commit()
            result_2 = (value_1["channel_id"], value_1["playlist_id"], value_1["playlist_title"])
            cur.execute(b, result_2)
        conn.commit()

        video_dict = video_2(channel_id)
        video_df = pd.DataFrame(video_dict)
        video_df["view_count"] = video_df["view_count"].astype("int64")
        video_df["like_count"] = video_df["like_count"].astype("int64")
        video_df["favorite_count"] = video_df["favorite_count"].astype("int64")
        video_df["video_duration"] = pd.to_timedelta(video_df["video_duration"])
        video_df["video_duration"] = video_df["video_duration"].dt.total_seconds().astype("int64")
        video_df['published_at'] = pd.to_datetime(video_df['published_at']).dt.date.astype("datetime64[ns]")
        # print(video_df.head(2))
        # print(video_df.info())
        cur.execute(
            "CREATE TABLE IF NOT EXISTS VIDEO(CHANNEL_ID VARCHAR(75), VIDEO_ID VARCHAR(100) PRIMARY KEY, VIDEO_TITLE "
            "TEXT, VIDEO_DESCRIPTION TEXT, VIDEO_DURATION INT, VIDEO_CAPTION VARCHAR(10), "
            "VIEW_COUNT INT, LIKE_COUNT INT, DISLIKE_COUNT INT, FAVOURITE_COUNT INT, COMMENT_COUNT INT, "
            "PUBLISHED_AT TIMESTAMP)")
        conn.commit()
        c = "INSERT INTO VIDEO(CHANNEL_ID, VIDEO_ID, VIDEO_TITLE, VIDEO_DESCRIPTION, VIDEO_DURATION, VIDEO_CAPTION, " \
            "VIEW_COUNT, LIKE_COUNT, DISLIKE_COUNT, FAVOURITE_COUNT, COMMENT_COUNT, PUBLISHED_AT) VALUES(%s, %s, %s, " \
            "%s, " \
            "%s, %s, %s, %s, %s, %s, %s, %s)"
        for index, value_2 in video_df.iterrows():
            val_2 = value_2["video_id"]
            cur.execute(f"SELECT * FROM VIDEO WHERE VIDEO_ID = '{val_2}'")
            y = cur.fetchall()
            if len(y) > 0:
                cur.execute(f"DELETE FROM VIDEO WHERE VIDEO_ID = '{val_2}'")
                conn.commit()
            result_3 = (
                value_2["channel_id"], value_2["video_id"], value_2["video_title"], value_2["video_description"],
                value_2["video_duration"], value_2["video_caption"], value_2["view_count"], value_2["like_count"],
                value_2["dislike_count"], value_2["favorite_count"], value_2["comment_count"], value_2["published_at"])
            cur.execute(c, result_3)
        conn.commit()

        comments_dict = video_comments()
        comments_df = pd.DataFrame(comments_dict)
        comments_df['published_at'] = pd.to_datetime(comments_df['published_at']).dt.date
        # print(comments_df.info())
        # print(comments_df.head(2))
        cur.execute("CREATE TABLE IF NOT EXISTS COMMENTS(VIDEO_ID VARCHAR(75), COMMENT_ID VARCHAR(75) PRIMARY KEY, "
                    "PUBLISHED_AT VARCHAR(30), COMMENTED_BY VARCHAR(50), COMMENT_GIVEN TEXT)")
        conn.commit()
        d = "INSERT INTO COMMENTS(VIDEO_ID, COMMENT_ID, PUBLISHED_AT,  COMMENTED_BY, COMMENT_GIVEN) VALUES(%s, %s, " \
            "%s, %s, %s)"
        for index, value_3 in comments_df.iterrows():
            val_3 = value_3["comment_id"]
            cur.execute(f"SELECT * FROM COMMENTS WHERE COMMENT_ID = '{val_3}'")
            z = cur.fetchall()
            if len(z) > 0:
                cur.execute(f"DELETE FROM COMMENTS WHERE COMMENT_ID = '{val_3}'")
                conn.commit()
            result_4 = (
                value_3["video_id"], value_3["comment_id"], value_3["published_at"], value_3["author_display_name"],
                value_3["display_text"])
            cur.execute(d, result_4)
        conn.commit()

else:
    st.title("YouTube Data Harvesting and Warehousing with MongoDB, SQL")
    st.subheader("Simplify and analyze the migrated data from the selected ten questions to gain valuable insights")

    question_list = ["Select", "What are the names of all the videos and their corresponding channels?",
                     "Which channels have the most number of videos, and how many videos do they have?",
                     "What are the top 10 most viewed videos and their respective channels?",
                     "How many comments were made on each video, and what are their corresponding video names?",
                     "Which videos have the highest number of likes, and what are their corresponding channel names?",
                     "What is the total number of likes and dislikes for each video, and what are their corresponding "
                     "video names?",
                     "What is the total number of views for each channel, and what are their corresponding channel "
                     "names?",
                     "What are the names of all the channels that have published videos in the year 2022?",
                     "What is the average duration of all videos in each channel, and what are their corresponding "
                     "channel names?",
                     "Which videos have the highest number of comments, and what are their corresponding channel names?"
                     ]
    question_selected = st.selectbox("Select the question from below options", options=question_list)

    if question_selected == "Select":
        st.write("   ")

    if question_selected == "What are the names of all the videos and their corresponding channels?":
        pass  # write sql query

    if question_selected == "Which channels have the most number of videos, and how many videos do they have?":
        pass

    if question_selected == "What are the top 10 most viewed videos and their respective channels?":
        pass

    if question_selected == "How many comments were made on each video, and what are their corresponding video names?":
        pass

    if question_selected == "Which videos have the highest number of likes, and what are their corresponding channel " \
                            "names?":
        pass

    if question_selected == "What is the total number of likes and dislikes for each video, and what are their " \
                            "corresponding video names?":
        pass

    if question_selected == "What is the total number of views for each channel, and what are their corresponding " \
                            "channel names?":
        pass

    if question_selected == "What are the names of all the channels that have published videos in the year 2022?":
        pass

    if question_selected == "What is the average duration of all videos in each channel, and what are their " \
                            "corresponding channel names?":
        pass

    if question_selected == "Which videos have the highest number of comments, and what are their corresponding " \
                            "channel names?":
        pass
