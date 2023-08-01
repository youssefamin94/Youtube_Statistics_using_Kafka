from kafka import KafkaConsumer
import json
import plotext
import pandas as pd

    
consumer = KafkaConsumer(
    'new_topic', 
    bootstrap_servers=['localhost:9092'], 
    auto_offset_reset='earliest', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

print("Starting Consumer")

# receive the message and parse the JSON string back into a DataFrame
for message in consumer:
    print("New message!")
    json_str = message.value
    df = pd.read_json(json_str, orient='records')
    print(df)
    plotext.subplots(1,3)
    
    plotext.subplot(1,1)
    plotext.title("Subscriber Count")
    plotext.multiple_bar(df['Channel_name'], df['Subscribers'])
    
    plotext.subplot(1,2)
    plotext.title("Number of Views")
    plotext.bar(df['Channel_name'],df['Views'])
    
    plotext.subplot(1,3)
    plotext.title("Total Number of Videos Posted")
    plotext.bar(df['Channel_name'],df['Total_videos'])
    
    plotext.show()
    
    

    
    
