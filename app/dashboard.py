import os
import pandas as pd
from FlaskApp.app import app
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
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

available_columns = df[['p_name','color','size','sku_id','in_stock','available_to_sell','available_to_sell_from_stock']]
available_products = df['p_name'].unique()
available_colors = df['color'].unique()
available_sizes = df['size'].unique()

dash_app = dash.Dash(server=app,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/")


product_option_list = sorted(available_columns['p_name'].to_list())
color_option_list = sorted(available_columns['color'].to_list())
size_option_list = sorted(available_columns['size'].to_list())

dash_app.layout = html.Div([
    dbc.Row([
        dbc.Card([
            dbc.CardBody([
                html.H1("Dashboard"),
                html.P('''
                     This is a dashboard for A.Emery
                     '''),
                ]),
            ],   
            style={"width": "18rem"},
        ),
    ]),
    dbc.Row([
        dbc.Card([
            dbc.CardBody([
                html.P("Product"),
                html.Div([
                    dcc.Dropdown(
                        id='product_option',
                        options=product_option_list,
                        value=[],
                        placeholder = 'All',
                        multi = True,
                        clearable = True
                    ),
                ]),
            ]),
        ]),
        dbc.Card([
            dbc.CardBody([
                html.P("Color"),
                html.Div([
                    dcc.Dropdown(
                        id='color_option',
                        options=color_option_list,
                        value=[],
                        placeholder = 'All',
                        multi = True,
                        clearable = True
                    ),
                ]),
            ]),
        ]),
        dbc.Card([
            dbc.CardBody([
                html.P("Size"),
                html.Div([
                    dcc.Dropdown(
                        id='size_option',
                        options=size_option_list,
                        value=[],
                        placeholder = 'All',
                        multi = True,
                        clearable = True
                    ),
                ]),
            ]),
        ]),

    ]),
    dbc.Row([
        dbc.Card([
            dbc.CardBody([
                dash_table.DataTable(
                    id='data_table',
                    #columns=[{"name": i, "id": i} for i in available_columns.columns],
                    data=available_columns.to_dict("records")
                )
            ]),
        ]),
    ])
])  

@dash_app.callback(
    Output('color_option', 'options'),
    Input('product_option', 'value')
)
def set_dropdown_options(product):
    dff = available_columns.copy()
    if product:
        dff = dff[dff['p_name'].isin(product)]
    return [{'label':x,'value':x} for x in dff['color'].unique()]
    return [{'label':x,'value':x} for x in dff['color'].unique()]

@dash_app.callback(
    Output('size_option', 'options'),
    [Input('product_option', 'value'),
    Input('color_option','value')]
)
def set_dropdown_options(product,color):
    dff = available_columns.copy()
    if product:
        dff = dff[dff['p_name'].isin(product)]
    if color:
        dff = dff[dff['color'].isin(color)]
    return [{'label':x,'value':x} for x in dff['size'].unique()]


@dash_app.callback (
        Output('data_table', 'data'),
        [Input('product_option', 'value'),
        Input('color_option','value'),
        Input('size_option','value')]
)
def update_table(v_product,v_color,v_size):
    if not v_product or v_product == 'All':
        v_product = product_option_list
    if not v_color or v_color == 'All':
        v_color = color_option_list
    if not v_size or v_size == 'All':
        v_size = size_option_list
    ddf = available_columns[(available_columns['p_name'].isin(v_product))&(available_columns['color'].isin(v_color))&(available_columns['size'].isin(v_size))]
    return ddf.to_dict("records")             