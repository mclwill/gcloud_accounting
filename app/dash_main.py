import dash
from dash import Dash, html, dcc

external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app = dash.Dash(server=app,use_pages=True,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/",suppress_call_exceptions=True) #previousy 'routes_pathname_prefix'

dash_app.layout = html.Div([
    dash.page_container
])

dash_app.validation_layout = html.Div([
	graphs.graphs_layout])