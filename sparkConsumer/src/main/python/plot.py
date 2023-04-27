import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pandas as pd
import numpy as np
import os
import datetime as dt
import json
from threading import Thread
from kafka import KafkaConsumer

# Create a figure and an axis object
fig, ax = plt.subplots()

# Initialize an empty list to store the timeseries data
prices = []
timestamps=[]

# Define the function to update the plot with new data
def update(num):
    # Generate some new data and append it to the y-axis data list
    # new_files = get_files()
    # for file in new_files:
    #     if file.endswith('.csv'):
    #         print('====' + path + '/' + file)
    #         try:
    # df_new = pd.read_csv(path + '/' + file)
    df_new = get_diff_df()
    df_new = df_new[df_new['symbol_coin'] == 'BTC'][['timestamp', 'price']]
    # df_new['epoch_timestamp'] = df_new['timestamp'].apply(lambda x: dt.datetime.timestamp(dt.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f')))
    df_new['epoch_timestamp'] = pd.to_datetime(df_new['timestamp']).apply(lambda x: int(x.timestamp()))
    df_new = df_new.sort_values(by=['epoch_timestamp'])
    y_data.extend(df_new['price'].tolist())
    x_data.extend(df_new['epoch_timestamp'].tolist())
    print("xxx=", x_data[0])
    print("yyy=", y_data[0])
    # Truncate the y-axis data to keep only the last 100 data points
    while len(y_data) > 1000:
        y_data.pop(0)
        x_data.pop(0)

            # except pd.errors.EmptyDataError:
            #     print(f"Skipping empty file: {file}")

    print("size ", len(y_data))
    # new_data = generate_data()
    # y_data.append(new_data)
    
    # Truncate the y-axis data to keep only the last 100 data points
    # if len(y_data) > 100:
    #     y_data.pop(0)
    
    # Clear the axis and plot the new data
    ax.clear()
    # for line in ax.lines:
    #     line.set_marker(None)
    ax.plot(x_data[-len(y_data):], y_data, 'b-')
    ax.set_xlim([x_data[0], x_data[-1]])
    ax.set_ylim([27300, 27800])

    # Remove the tick markers
    # ax.tick_params(axis='x', which='both', length=0)

    # line.set_data(x_data, y_data)
    # ax.relim()
    # ax.autoscale_view()
    # return line,
    
# Create an animation object
ani = animation.FuncAnimation(fig, update, interval=1000)

# Show the plot
plt.show()

def startKafkaConsumer():
    consumer = KafkaConsumer(
        'feature_vector',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        print(message.value)    


if __name__ == "main":
    startKafkaConsumer()