import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import numpy as np

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
    # Generate new random data
    num_records = 100
    timestamps = pd.date_range(pd.Timestamp.now(), periods=num_records, freq='S')
    prices = np.random.normal(10000, 1000, num_records).round(2)
    data = {'Timestamp': timestamps, 'Price': prices}
    df = pd.DataFrame(data)

    # Create plotly graph object
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['Timestamp'], y=df['Price'], mode='lines', name='Bitcoin Price'))

    # Set graph layout
    fig.update_layout(
        xaxis_title='Time',
        yaxis_title='Bitcoin Price (USD)',
        margin=dict(l=40, r=20, t=30, b=30)
    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)

