import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd

# Create a Dash application
app = dash.Dash(__name__)

# Sample DataFrame for demonstration
df = pd.DataFrame({
    'date': pd.date_range(start='2021-01-01', periods=100),
    'cases': range(100),
    'deaths': [x * 0.1 for x in range(100)]
})

# Create a basic Plotly figure
fig = px.line(df, x='date', y='cases', title='COVID-19 Cases Over Time')

# Define the layout of the app
app.layout = html.Div([
    html.H1("COVID-19 Dashboard"),

    dcc.Graph(
        id='example-graph',
        figure=fig
    ),

    html.Div([
        html.Label("Select Metric:"),
        dcc.Dropdown(
            id='metric-dropdown',
            options=[
                {'label': 'Cases', 'value': 'cases'},
                {'label': 'Deaths', 'value': 'deaths'}
            ],
            value='cases'
        )
    ])
])

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
