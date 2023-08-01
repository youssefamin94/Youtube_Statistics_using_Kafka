import os
import google.auth
from googleapiclient.discovery import build
import pandas as pd 
import seaborn as sns
from kafka import KafkaProducer
from data import get_registered_user
import json
import time
import plotext

producer = KafkaProducer(bootstrap_servers= ['localhost:9092'],value_serializer=lambda x: json.dumps(x).encode('utf-8')) 


# Set up authentication
api_key = "AIzaSyDVll6bMU_ahQN2nJ33FRQCi1C6DIEQg2o"
youtube = build('youtube', 'v3', developerKey=api_key)

def get_channel_stats(youtube,channel_id):

    request = youtube.channels().list(
        part = 'snippet,contentDetails,statistics',
        id = channel_id)

    response = request.execute()

    data = dict(Channel_name = response['items'][0]['snippet']['title'],
                Subscribers = response['items'][0]['statistics']['subscriberCount'],
                Views = response['items'][0]['statistics']['viewCount'],
                Total_videos = response['items'][0]['statistics']['videoCount'])

    return data

n = int(input("How many channels would you like to compare? "))
channels_list = []
for i in range(n):
    channels_list.append(input("Please enter the channel id: "))

channels_data = []
for channel in channels_list:
    channels_data.append(get_channel_stats(youtube,channel))

df = pd.DataFrame(channels_data)
df['Subscribers'] = pd.to_numeric(df['Subscribers'])
df['Views'] = pd.to_numeric(df['Views'])
df['Total_videos'] = pd.to_numeric(df['Total_videos'])

print(df)

json_str = df.to_json(orient='records')
producer.send('new_topic', value=json_str)
print("Sent to consumer!")


