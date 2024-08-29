import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc

external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']



dash.get_app().layout = html.Div([
    dash.page_container
])

#dash_app.get_app.validation_layout = html.Div([
#	graphs.graphs_layout])