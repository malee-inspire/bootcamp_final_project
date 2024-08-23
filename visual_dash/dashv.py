import dash
from dash import dcc
from dash import html
import plotly.express as px
from dash.dependencies import Input, Output
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import requests

app = dash.Dash(__name__)


# Load data from Snowflake using FastAPI
def load_data():
    response = requests.get('http://localhost:8000/query_data/?table=JHU_DASHBOARD_COVID_19_GLOBAL')
    return response.json()['data']


data = load_data()

# Process data into a DataFrame (Assuming data is JSON format)
import pandas as pd

df = pd.DataFrame(data)

# Infection Rate Visualization
fig_infection = px.line(df, x='DATE', y='CONFIRMED', color='COUNTRY_REGION',
                        title="Infection Rates Over Time")

# Mortality Rate Visualization
fig_mortality = px.line(df, x='DATE', y='DEATHS', color='COUNTRY_REGION',
                        title="Mortality Rates Over Time")

# Layout
app.layout = html.Div([
    html.H1("COVID-19 Dashboard"),

    # Infection Rate Graph
    dcc.Graph(id='infection-rate-chart', figure=fig_infection),

    # Mortality Rate Graph
    dcc.Graph(id='mortality-rate-chart', figure=fig_mortality),

    # Demographic Breakdown
    html.H3("Demographic Breakdown"),
    html.Div(id='demographic-display'),

    # Annotation Input
    dcc.Input(id='annotation-input', type='text', placeholder='Enter annotation...'),
    html.Button('Submit', id='submit-annotation', n_clicks=0),
    html.Div(id='annotations-display')
])


# Update Demographic Data Display
@app.callback(
    Output('demographic-display', 'children'),
    [Input('infection-rate-chart', 'hoverData')]
)
def update_demographics(hoverData):
    if hoverData is None:
        return "Hover over a point to see demographic data."

    point_data = hoverData['points'][0]
    country = point_data['customdata'][0]

    response = requests.get(f'http://localhost:8000/query_data/?table=DEMOGRAPHICS&country={country}')
    demographics = response.json()['data']

    return [
        html.P(f"Country: {country}"),
        html.P(f"Total Population: {demographics['TOTAL_POPULATION']}"),
        html.P(f"Female Population: {demographics['TOTAL_FEMALE_POPULATION']}"),
        html.P(f"Male Population: {demographics['TOTAL_MALE_POPULATION']}")
    ]


# Handle Annotations
@app.callback(
    Output('annotations-display', 'children'),
    Input('submit-annotation', 'n_clicks'),
    Input('annotation-input', 'value')
)
def update_annotations(n_clicks, annotation_text):
    if n_clicks > 0 and annotation_text:
        new_annotation = {
            "user_id": "example_user_id",
            "visualization_id": "infection_rate_chart",
            "data_point_id": "example_data_point_id",
            "annotation_text": annotation_text,
            "location_data": {"country": "USA"},
            "filter_criteria": {"date_range": {"start_date": "2020-01-01", "end_date": "2020-12-31"}}
        }

        response = requests.post('http://localhost:8000/annotations/', json=new_annotation)
        return f"Annotation added: {annotation_text}"

    return "No annotations yet."


# Fetch historical data
def load_data():
    response = requests.get('http://localhost:8000/query_data/?table=JHU_DASHBOARD_COVID_19_GLOBAL')
    return response.json()['data']


data = load_data()
df = pd.DataFrame(data)
df['DATE'] = pd.to_datetime(df['DATE'])

# Create an initial infection rate plot
fig_infection = px.line(df, x='DATE', y='CONFIRMED', color='COUNTRY_REGION',
                        title="Infection Rates Over Time")

# Layout
app.layout = html.Div([
    html.H1("COVID-19 Dashboard"),

    # Infection Rate Graph
    dcc.Graph(id='infection-rate-chart', figure=fig_infection),

    # Forecast Graph
    dcc.Graph(id='forecast-chart'),

    # Input for forecast
    dcc.Input(id='forecast-country', type='text', placeholder='Enter country...'),
    html.Button('Submit Forecast', id='submit-forecast', n_clicks=0)
])


# Update Forecast
@app.callback(
    Output('forecast-chart', 'figure'),
    [Input('submit-forecast', 'n_clicks')],
    [Input('forecast-country', 'value')]
)
def update_forecast(n_clicks, country):
    if n_clicks > 0 and country:
        response = requests.get(
            f'http://localhost:8000/forecast/?table=JHU_DASHBOARD_COVID_19_GLOBAL&country={country}')
        forecast = response.json()['forecast']

        forecast_df = pd.DataFrame(forecast)
        forecast_df['ds'] = pd.to_datetime(forecast_df['ds'])

        # Plot forecast
        fig_forecast = px.line(forecast_df, x='ds', y='yhat',
                               title=f"Forecast for {country}")
        fig_forecast.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_lower'], mode='lines', name='Lower Bound')
        fig_forecast.add_scatter(x=forecast_df['ds'], y=forecast_df['yhat_upper'], mode='lines', name='Upper Bound')

        return fig_forecast

    return px.line(title="Submit a country to view the forecast")


# Layout
app.layout = html.Div([
    html.H1("COVID-19 Dashboard"),

    # Infection Rate Graph
    dcc.Graph(id='infection-rate-chart'),

    # Forecast Graph
    dcc.Graph(id='forecast-chart'),

    # Clustering Graph
    dcc.Graph(id='clustering-chart'),

    # Input for clustering
    html.Button('Fetch Clusters', id='fetch-clusters', n_clicks=0)
])


# Update Clustering
@app.callback(
    Output('clustering-chart', 'figure'),
    Input('fetch-clusters', 'n_clicks')
)
def update_clustering(n_clicks):
    if n_clicks > 0:
        response = requests.get('http://localhost:8000/clustering/')
        clusters = response.json()['clusters']

        cluster_df = pd.DataFrame(clusters)
        fig_clustering = px.scatter(cluster_df, x='CONFIRMED', y='DEATHS', color='Cluster',
                                    title="Clustering of Regions by COVID-19 Metrics")

        return fig_clustering

    return px.scatter(title="Fetch clusters to view the data")

if __name__ == '__main__':
    app.run_server(debug=True)
