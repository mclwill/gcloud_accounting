import dash
from dash import html
import plotly.express as px

#import FlaskApp.app.pages.dashboard

dash.register_page(__name__)

def layout(**kwargs):
	'''plots = kwargs.pop('plots',None)
	
	df = dashboard.stock_info_df

	if plots:'''

	if kwargs.keys() : 
		return html.Div([
	        html.Div(
	            'Key:' + str(k) + ' : ' + 'Value: ' + str(v)
	        ) for k,v in kwargs.items()
	    ])
	else:
		return html.Div([
	        html.Div('No parameters in URL')
	    ])