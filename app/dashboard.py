import os
import pandas as pd
from FlaskApp.app import app
from flask_login import login_required
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table
from dash.dependencies import Input, Output
from dash_table import DataTable
from dash.exceptions import PreventUpdate
import plotly.express as px
import traceback

import FlaskApp.app.common as common

external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']

customer = 'aemery'


data_store_folder = common.data_store[customer]
stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
orders_file_path = os.path.join(data_store_folder,'data_orders.csv')

dash_app = dash.Dash(server=app,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/") #previousy 'routes_pathname_prefix'

#common.logger.info (str(app.view_functions))

for view_func in app.view_functions:
    if view_func.startswith(dash_app.config['routes_pathname_prefix']):
        app.view_functions[view_func] = login_required(app.view_functions[view_func])

def serve_layout():
    try:
        #collect data in serve_layout so that latest is retrieved from data_store
        global available_columns,available_products,available_colors,available_sizes
        global product_option_list,color_option_list,size_option_list

        byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
        if byte_stream:
            df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            df = pd.DataFrame() #start with empty dataframe

        if df.empty:
            return html.Div(
                html.P('No Data to Display - need to check Data Store')
            )

        df['url_markdown'] = df['url'].map(lambda a : "[![Image Not Available](" + str(a) + ")](https://aemery.com)")

        available_columns = df[['url_markdown','date','season','p_name','color','size','sku_id','in_stock','available_to_sell','available_to_sell_from_stock']]
        col_title_mapping = {'url_markdown':'Image','date':'Date','season':'Season(s)','p_name':'Product','color':'Colour','size':'Size','sku_id':'SKU','in_stock':'In Stock','available_to_sell':'Available To Sell','available_to_sell_from_stock':'Available To Sell From Stock'}
        available_columns = available_columns[available_columns['date'] == available_columns['date'].max()]
        available_products = df['p_name'].unique()
        available_colors = df['color'].unique()
        available_sizes = df['size'].unique()

        product_option_list = sorted(available_columns['p_name'].unique().tolist())
        color_option_list = sorted(available_columns['color'].unique().tolist())
        size_option_list = sorted(available_columns['size'].unique().tolist())
        season_option_list = []
        for ss in available_columns['season'].to_list():
            for s in ss.split(','):
                if s not in season_option_list:
                    season_option_list.append(s)

        return html.Div([
            dbc.Row([
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.H1("Dashboard"),
                            html.P('''
                                 This is a dashboard for A.Emery
                                 '''),
                        ]),   
                    ],className="border-0 bg-transparent"),
                    width={"size":4}
                ),
                dbc.Col(
                    dbc.Button("Logout",href='/logout',color='light',size='lg',external_link=True,),
                    width={"size":1,'offset':12}
                )
            ],justify='evenly'),
            dbc.Row([
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Season"),
                            html.Div([
                                dcc.Dropdown(
                                    id='season_option',
                                    options=season_option_list,
                                    value=[],
                                    placeholder = 'All',
                                    multi = True,
                                    clearable = True
                                ),
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                ),
                dbc.Col(
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
                        ],className="border-0 bg-transparent"),
                ),
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Colour"),
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
                    ],className="border-0 bg-transparent"),
                ),
                dbc.Col(
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
                    ],className="border-0 bg-transparent"),
                ),
                
            ]),
            dbc.Row([
                dbc.Card([
                    dbc.CardBody([
                        dash_table.DataTable(
                            id='data_table',
                            columns=[{"name": col_title_mapping[i], "id": i, 'presentation':'markdown'} if ('markdown' in i) else {"name": col_title_mapping[i], "id": i} for i in available_columns.columns],
                            data=available_columns.to_dict("records"),
                            style_cell_conditional = [
                                {
                                    'if':{'column_id':i},
                                    'textAlign':'center'
                                } for i in ['url_markdown']
                            ],
                            css=[dict(selector= "p", rule= "margin: 0; text-align: center")]
                        )
                    ]),
                ]),
            ])
        ])
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '/nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

@dash_app.callback(
    Output('product_option', 'options'),
    Input('season_option', 'value')
)
def set_dropdown_options(season):
    dff = available_columns.copy()
    if season:
        seasons = []
        for ss in season:
            for s in ss.split(','):
                if s not in seasons:
                    seasons.append(s)
        dff = dff[dff['season'].str.contains('|'.join(seasons))]
    return [{'label':x,'value':x} for x in dff['product'].unique()]


@dash_app.callback(
    Output('color_option', 'options'),
    Input('product_option', 'value')
)
def set_dropdown_options(product):
    dff = available_columns.copy()
    if product:
        dff = dff[dff['p_name'].isin(product)]
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
        [Input('season_option','value'),
        Input('product_option', 'value'),
        Input('color_option','value'),
        Input('size_option','value')]
)
def update_table(v_season,v_product,v_color,v_size):
    if not v_season or v_season == 'All':
        v_season = season_option_list
    else:
        v_seasons = []
        for ss in v_season:
            for s in ss.split(','):
                if s not in v_seasons:
                    v_seasons.append(s)
    if not v_product or v_product == 'All':
        v_product = product_option_list
    if not v_color or v_color == 'All':
        v_color = color_option_list
    if not v_size or v_size == 'All':
        v_size = size_option_list
    
    dff = available_columns[(available_columns['season'].str.contains('|'.join(v_seasons)))&(available_columns['p_name'].isin(v_product))&(available_columns['color'].isin(v_color))&(available_columns['size'].isin(v_size))]
    return dff.to_dict("records")   

dash_app.layout = serve_layout
       