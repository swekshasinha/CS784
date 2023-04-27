
import dash
import json
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from kafka import KafkaConsumer
import pandas as pd

import queue
import threading
# Kafka-related variables
consumer = KafkaConsumer(
        'feature_vector',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
print("===consumer ready==")

q = queue.Queue()

# Define function to consume messages and put them in the queue
def consume():
    while True:
#        message = next(consumer)
        for message in consumer:
            q.put(message.value, block=False)
            print("q len from consumer = ", q.qsize())
#        print("q len from consumer = ", q.qsize())
# Start Kafka consumer thread
t = threading.Thread(target=consume)
t.daemon = True
t.start()


# Initialize Dash app
app = dash.Dash(__name__)

# Define layout for the app
app.layout = html.Div(children=[
    html.H1(children='Bitcoin Prices'),

    dcc.Graph(
        id='live-graph',
        animate=True
    ),

    dcc.Interval(
        id='interval-component',
        interval=10*1000,  # update interval in milliseconds
        n_intervals=0
    )
])

# Define callback function to update graph with new data
@app.callback(dash.dependencies.Output('live-graph', 'figure'),
              [dash.dependencies.Input('interval-component', 'n_intervals')])
def update_graph(n):
    print("===update graph==")

##{'name_coin': 'Binance USD', 'min_price': 1.0008983613193354, 'max_price': 1.0024252096171107, 'open': 1.0008983613193354, 'close': 1.0024252096171107, 'start_time': '2023-04-26T19:05:30.000-05:00', 'end_time': '2023-04-26T19:10:30.000-05:00', 'RoI': 0.15254778674656405}
#
#    # Convert new data to a pandas DataFrame
#    df = pd.DataFrame(new_data, columns=['name_coin', 'min_price', 'max_price', 'open', 'close', 'Timestamp', 'end_time', 'ROI'])
#    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')
#    df.set_index('Timestamp', inplace=True)

    num_records = 1000
    records = []
    print("q len ===== = ", q.qsize())
    
    for i in range(num_records):
        try:
            message = q.get_nowait()
            records.append(message)
#            if (message['name_coin'] != 'Bitcoin'): continue
        except queue.Empty:
            print("Q empty")
            break

    df = pd.DataFrame(records)

    # iterating the columns
    print("printing columns===",  len(df.columns), "rec len ===", len(records))
    if (len(df.columns) == 0):
        return dash.no_update
    for col in df.columns:
        print("col= ", col)
        
    print("# rows before = ", len(df.index))
    print(df)
    df = df[df['name_coin'] == 'Bitcoin'][['start_time', 'close']]
    print("# rows after = ", len(df.index))

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['close'], mode='lines', name='Bitcoin Price'))

    # Set graph layout
    fig.update_layout(
        xaxis_title='Time',
        yaxis_title='Bitcoin Price (USD)',
        margin=dict(l=40, r=20, t=30, b=30)
    )


    # Create plotly graph object
#    fig = go.Figure()
#    fig.add_trace(go.Scatter(x=df.index, y=df['close'], mode='lines', name='Bitcoin Price'))
#    print("===update layout==")
#    # Set graph layout
#    fig.update_layout(
#        xaxis_title='Time',
#        yaxis_title='Bitcoin Price (USD)',
#        margin=dict(l=40, r=20, t=30, b=30)
#    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)

