import dash
from dash import Dash, html, dcc



dash.get_app().layout = html.Div([
    dash.page_container
])

dash.get_app().validation_layout = html.Div([
	graphs.layout])