import os
from FlaskApp.app import dash_app
import dash_html_components as html
import dash_core_components as dcc
import dash_table
from dash.dependencies import Input, Output
from dash_table import DataTable
from dash.exceptions import PreventUpdate
import plotly.express as px

import FlaskApp.app.common as common

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

customer = 'aemery'

data_store_folder = common.data_store[customer]
stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
orders_file_path = os.path.join(data_store_folder,'data_orders.csv')

byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
if byte_stream:
    df = pd.read_csv(byte_stream,sep='|',index_col=False)
else:
    df = pd.DataFrame() #start with empty dataframe

available_columns = df[['p_identifier','p_name','color','size','sku_id']]
available_products = df['p_name'].unique()



dash_app.layout = html.Div([
        html.Div([
                html.H1("Dashboard"),
                html.Div('''
                         This is a dashboard for A.Emery
                         '''),
                html.Div('')
        ]),
        html.Div([
                dcc.Dropdown(
                        id='select_column',
                        options=[{'label':i, 'value': i} for i in available_produtcs],
                        value='THE ELI SANDAL',
                        #multi=True
                )
        ]),
        dash_table.DataTable(
            id='data_table',
            columns=[{"name": i, "id": i} for i in ingredients_tab_data.columns],
            data=ingredients_tab_data.to_dict("rows")
        )
])  

@dash_app.callback (
	    Output('data_table', 'data'),
        Input('select_column', 'value')
)

def update_table(value):
    data=available_columns[available_columns['p_name'].isin(value)]
    return data.to_dict("rows")             