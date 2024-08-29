import dash
from dash import html
import plotly.express as px

from FlaskApp.app.data_store import stock_info_df

dash.register_page(__name__)

def layout(**kwargs):
	rows = kwargs.pop('plots',None)
	data = kwargs.pop('data',None)
	
	'''df = stock_info_df

	if len(plots) > 0 :
		return ([
			html.Div([
				dcc.Graph(
					id='stock_info_df',fig=)])])'''

	if kwargs.keys() : 
		return html.Div([
	        html.Div(
	            'Key :' + str(k) + ' : ' + 'Value : ' + str(v)
	        ) for k,v in kwargs.items()
	    ])
	else:
		return html.Div([
	        html.Div('No parameters in URL')
	    ])