import dash
from dash import html

dash.register_page(__name__)

def layout(**kwargs):
	return html.Div([
        html.Div(
            'Key:' + str(k) + ' : ' + 'Value: ' + str(v)
        ) for k,v in kwargs.items()
    ])