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
from datetime import datetime

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

def get_earliest_product_inventory_date(x,df):
    return df['date'][df['sku_id'] == x['sku_id']].min()

def serve_layout():
    #global season_latest_stock_info
    global latest_stock_info

    try:
        #collect data in serve_layout so that latest is retrieved from data_store
        global latest_stock_info
        global product_option_list,color_option_list,size_option_list,season_option_list

        byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
        if byte_stream:
            df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            df = pd.DataFrame() #start with empty dataframe

        if df.empty:
            return html.Div(
                html.P('No Data to Display - need to check Data Store')
            )
        df['date'] = pd.to_datetime(df['date'])
        df['url_markdown'] = df['url'].map(lambda a : "[![Image Not Available](" + str(a) + ")](https://aemery.com)")  #get correctly formatted markdown to display images in data_table
        df['e_date'] = df.apply(lambda row: get_earliest_product_inventory_date(row,df=df),axis=1).dt.date #get earliest inventory date for each sku_id
        latest_stock_info = df[['url_markdown','e_date','date','season','p_name','color','size','available_to_sell']]
        col_title_mapping = {'url_markdown':'Image','e_date':'Earliest Data','date':'Date','season':'Season(s)','p_name':'Product','color':'Colour','size':'Size','sku_id':'SKU','in_stock':'In Stock','available_to_sell':'Available To Sell','available_to_sell_from_stock':'Available To Sell From Stock'}
        latest_date = latest_stock_info['date'].max().to_datetime()
        earliest_date = latest_stock_info['date'].min().to_datetime()
        common.logger.info(str(type(latest_date)) + str(latest_date))
        latest_stock_info = latest_stock_info[latest_stock_info['date'] == latest_date]
        latest_stock_info.drop('date',axis=1,inplace=True)

        product_option_list = sorted(latest_stock_info['p_name'].unique().tolist())
        color_option_list = sorted(latest_stock_info['color'].unique().tolist())
        size_option_list = sorted(latest_stock_info['size'].unique().tolist())
        season_option_list = []
        for ss in latest_stock_info['season'].to_list():
            for s in ss.split(','):
                if s not in season_option_list:
                    season_option_list.append(s)
        season_option_list.sort()

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
                            html.P("Start Date - earliest is " + earliest_date.strftime('%d-%m-%Y)')),
                            html.Div([
                                dcc.DatePickerSingle(
                                    id='start_date_picker',
                                    min_date_allowed = earliest_date.date,
                                    max_date_allowed = latest_date.date,
                                    initial_visible_month = earliest_date.date,
                                    date = earliest_date.date
                                ),
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                ),
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Season(s)"),
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
                                    #placeholder = 'All',
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
                                    #placeholder = 'All',
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
                                    #placeholder = 'All',
                                    multi = True,
                                    clearable = True
                                ),
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                ),   
            ]),
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Div(id='dd-output-container',children='Data Update Complete')
                    ],style={'backgroundColor':'red','color':'white'})
                ]),
            ], align='center'),
            dbc.Row([
                dbc.Card([
                    dbc.CardBody([
                        dash_table.DataTable(
                            id='data_table',
                            columns=[{"name": col_title_mapping[i], "id": i, 'presentation':'markdown'} if ('markdown' in i) else {"name": col_title_mapping[i], "id": i} for i in latest_stock_info.columns],
                            data=latest_stock_info.to_dict("records"),
                            style_cell_conditional = [
                                {
                                    'if':{'column_id':i},
                                    'textAlign':'center'
                                } for i in ['url_markdown']
                            ],
                            css=[dict(selector= "p", rule= "margin: 0; text-align: center")],
                            sort_action = 'native',
                        )
                    ]),
                ]),
            ])
        ])
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 



@dash_app.callback(
    Output('product_option', 'options'),
    Input('season_option', 'value')
)
def set_dropdown_options(season):
    global latest_stock_info
    dff = latest_stock_info.copy()
    if season:
        seasons = []
        for ss in season:
            for s in ss.split(','):
                if s not in seasons:
                    seasons.append(s)
        dff = dff[dff['season'].str.contains('|'.join(seasons))]
    return [{'label':x,'value':x} for x in dff['p_name'].unique()]


@dash_app.callback(
    Output('color_option', 'options'),
    Input('product_option', 'value')
)
def set_dropdown_options(product):
    global latest_stock_info
    dff = latest_stock_info.copy()
    if product:
        dff = dff[dff['p_name'].isin(product)]
    return [{'label':x,'value':x} for x in dff['color'].unique()]

@dash_app.callback(
    Output('size_option', 'options'),
    [Input('product_option', 'value'),
    Input('color_option','value')]
)
def set_dropdown_options(product,color):
    global latest_stock_info
    dff = latest_stock_info.copy()
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
        Input('size_option','value')],
        running=[(Output("dd-output-container","children"),'Data Being Updated.....Please Wait', 'Data Update Complete'),
                 (Output("dd-output-container","style"),{'backgroundColor':'red','color':'white'},{'backgroundColor':'white','color':'black'})]
)
def update_table(v_season,v_product,v_color,v_size):
    global latest_stock_info

    try:
        dff = latest_stock_info.copy()
        group_list = []
        sum_list = ['available_to_sell']
        present_list = latest_stock_info.columns.values.tolist()
        if not v_season or v_season == 'All':
            v_seasons = season_option_list
        else:
            v_seasons = []
            for ss in v_season:
                for s in ss.split(','):
                    if s not in v_seasons:
                        v_seasons.append(s)
        dff = dff[(dff['season'].str.contains('|'.join(v_seasons)))]
        '''if v_product == 'All':
            v_product = product_option_list
        if v_color == 'All':
            v_color = color_option_list
        if v_size == 'All':
            v_size = size_option_list'''
        if v_product : 
            dff = dff[dff['p_name'].isin(v_product)]
        if v_color :
            dff = dff[dff['color'].isin(v_color)]
        if v_size :
            dff = dff[dff['size'].isin(v_size)]
        
        #df = latest_stock_info[(latest_stock_info['season'].str.contains('|'.join(v_seasons)))]
        #dff = latest_stock_info[(latest_stock_info['season'].str.contains('|'.join(v_seasons)))|(latest_stock_info['p_name'].isin(v_product))|(latest_stock_info['color'].isin(v_color))|(latest_stock_info['size'].isin(v_size))]
        #common.logger.info('1 list' + str(group_list) + str(sum_list) + str(present_list))
        '''if not v_product:
            group_list.append('season')
            present_list.remove('p_name')
        if not v_color:
            group_list.append('p_name')
            present_list.remove('color')'''
        if not v_size:
            group_list.append('color')
        if not v_color:
            group_list.append('p_name')
        if not v_product:
            group_list.append('season')

        #common.logger.info('2 list' + str(group_list) + str(sum_list) + str(present_list))
        agg_dict = {}
        for x in present_list:
            if x not in group_list:
                if x in sum_list:
                    agg_dict[x] = 'sum'
                else:
                    agg_dict[x] = 'first'
        #common.logger.info('Pre Group By ' + str(dff.head()))
        if group_list:
            df_grouped = dff.groupby(group_list).agg(agg_dict).reset_index()
        else:
            df_grouped = dff
        #common.logger.info('Post Group by ' + str(df_grouped.head()))
        return df_grouped[present_list].to_dict("records")
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

dash_app.layout = serve_layout
       