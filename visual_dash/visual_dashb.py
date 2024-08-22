import visual_dashb
from dash import dcc, html
import plotly.express as px
import pandas as pd
from routes.snow_conn import snow_create_connection

# df = pd.read_sql('SELECT * FROM ...', connection)
# fig = px.line(df, x='DATE', y='DAILY_CASES', color='COUNTRY_REGION')
# fig.show()

#### Infection Rate Visualization

query = """
SELECT DATE, COUNTRY_REGION, CONFIRMED
FROM JHU_COVID_19_TIMESERIES
WHERE COUNTRY_REGION = 'USA'
"""

try:
    conn = snow_create_connection()
    print("connection was success!")
except Exception as e:
    print(f'snow flake does not connect: {e}')

df = pd.read_sql(query, conn)

# Plotly Figure
fig = px.line(df, x='DATE', y='CONFIRMED', title='COVID-19 Infection Rates in USA')

# Dash Layout
app = dash.Dash(__name__)
app.layout = html.Div(children=[
    html.H1(children='COVID-19 Dashboard'),
    dcc.Graph(id='infection-rate-graph', figure=fig)
])

if __name__ == '__main__':
    app.run_server(debug=True)


def dcc():
    return None