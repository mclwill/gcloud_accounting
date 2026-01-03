import dash
from dash import Dash, html, dcc

dash_app = dash.get_app()

dash_app.layout = html.Div([
    dash.page_container
])

#dash.get_app().validation_layout = html.Div([
#	graphs.layout])
