import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from kafka import KafkaConsumer
import pandas as pd

# Kafka-related variables
bootstrap_servers = ['localhost:9092']
topic_name = 'bitcoin_prices'
consumer_group_id = 'dashboard_group'
consumer = KafkaConsumer(feature_vector,
                         group_id=consumer_group_id,
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest')

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
        interval=1*1000,  # update interval in milliseconds
        n_intervals=0
    )
])

# Define callback function to update graph with new data
@app.callback(dash.dependencies.Output('live-graph', 'figure'),
              [dash.dependencies.Input('interval-component', 'n_intervals')])
def update_graph(n):
    # Read new data from Kafka topic
    new_data = []
    for msg in consumer:
        record = msg.value.decode('utf-8').split(',')
        new_data.append(record)
        if len(new_data) >= 100:  # limit number of records to avoid slowing down the app
            break

    # Convert new data to a pandas DataFrame
    df = pd.DataFrame(new_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume_(BTC)', 'Volume_(Currency)', 'Weighted_Price'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')
    df.set_index('Timestamp', inplace=True)

    # Create plotly graph object
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['Close'], mode='lines', name='Bitcoin Price'))

    # Set graph layout
    fig.update_layout(
        xaxis_title='Time',
        yaxis_title='Bitcoin Price (USD)',
        margin=dict(l=40, r=20, t=30, b=30)
    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)

