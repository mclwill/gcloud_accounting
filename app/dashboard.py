import os
import pandas as pd
from FlaskApp.app import app
from flask_login import login_required
import dash
#import dash_html_components as html
#import dash_core_components as dcc
from dash import html
from dash import dcc
from dash import dash_table
import dash_bootstrap_components as dbc
#import dash_table
from dash.dependencies import Input, Output
from dash_table import DataTable
from dash.exceptions import PreventUpdate
import plotly.express as px
import traceback
from datetime import datetime, date, time, timedelta
from dateutil import tz
import numpy as np

import FlaskApp.app.common as common

external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']

customer = 'aemery'

utc_zone = tz.tzutc()
to_zone = tz.gettz('Australia/Melbourne')

data_store_folder = common.data_store[customer]
stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
orders_file_path = os.path.join(data_store_folder,'data_orders.csv')
po_file_path = os.path.join(data_store_folder,'data_po.csv')

dash_app = dash.Dash(server=app,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/") #previousy 'routes_pathname_prefix'

#common.logger.info (str(app.view_functions))

for view_func in app.view_functions:
    if view_func.startswith(dash_app.config['routes_pathname_prefix']):
        app.view_functions[view_func] = login_required(app.view_functions[view_func])

def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)

def get_start_of_previous_week(date_value):
    weekday = date_value.weekday()
    sunday_delta = timedelta(days=weekday,weeks=1)
    return date_value - sunday_delta

def get_earliest_date(row,df):
    return df['date'][df['sku_id'] == row['sku_id']].min()

def get_base_available_to_sell(df):
    global base_start_date
    #common.logger.info(str(df[(df['sku_id'] == row['sku_id'])&(df['date']==base_start_date)].loc[:,'available_to_sell'].values))
    return_df = df[['ean','available_to_sell']][(df['date']==base_start_date)]
    return_df.rename({'available_to_sell':'base_available_to_sell'},inplace=True,axis=1)
    return_df.set_index('ean',inplace=True)
    return return_df['base_available_to_sell']

def get_last_week_orders(df,):
    global start_of_previous_week,end_of_previous_week
    return df.assign(result=np.where((df['date_shipped']>=start_of_previous_week)&(df['date_shipped']<=end_of_previous_week),df['qty_shipped'],0)).groupby('ean').agg({'result':sum})

def get_orders_since_start(df):
    global base_start_date
    return df.assign(result=np.where(df['date_shipped']>=base_start_date,df['qty_shipped'],0)).groupby('ean').agg({'result':sum})

def get_additonal_purchases(df):
    global base_start_date
    return df.assign(result=np.where(df['date_received']>=base_start_date,df['qty_received'],0)).groupby('ean').agg({'result':sum})



    #df['qty_received'].fillna(0,inplace=True)
    #groups = df.groupby(by='ean')
    #common.logger.info(str(groups.head()))
    #return groups.apply(lambda g: g[(g['date_received']>=base_start_date)]['qty_received'].sum())

def serve_layout():
    #global season_stock_info_df
    global stock_info_df,display_columns,latest_date,earliest_date
    global base_start_date,end_season_date,start_of_previous_week,end_of_previous_week
    global product_option_list,color_option_list,size_option_list,season_option_list

    try:
        #collect data in serve_layout so that latest is retrieved from data_store

        aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)

        #get stock info from data store
        byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
        if byte_stream:
            stock_info_df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            stock_info_df = pd.DataFrame() #start with empty dataframe

        if stock_info_df.empty:
            return html.Div(html.P('No Stock Data retrieved from Data Store'))

        
        stock_info_df['date'] = pd.to_datetime(stock_info_df['date']) #convert date column to_datetime
        latest_date = stock_info_df['date'].max().to_pydatetime().date() #get latest and earliest as pure dates (ie. drop time info)
        earliest_date = stock_info_df['date'].min().to_pydatetime().date()
        base_start_date = earliest_date #establish base date for calculating percentages etc (ie. start of season) as earliest date ---> need to modify this when this can be set through the dashboard using 'Start Date'
        end_season_date = last_day_of_month(aest_now.date()) #default data as end of this month
        start_of_previous_week = get_start_of_previous_week(aest_now.date())  #this should be the Monday of the previous week
        end_of_previous_week = start_of_previous_week + timedelta(days=6) #this should be the Sunday of the previous week

        #get order info from data store
        byte_stream = common.read_dropbox_bytestream(customer,orders_file_path)
        if byte_stream:
            orders_df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            orders_df = pd.DataFrame() #start with empty dataframe

        if orders_df.empty:
            return html.Div(html.P('No Orders Data retrieved from Data Store'))

        #get po info from data store
        byte_stream = common.read_dropbox_bytestream(customer,po_file_path)
        if byte_stream:
            po_df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            po_df = pd.DataFrame() #start with empty dataframe

        if po_df.empty:
            return html.Div(html.P('No Purchase Orders Data retrieved from Data Store'))

        #convert date column to_datetime in all dfs - ie drop time info
        po_df['date_received'] = pd.to_datetime(po_df['date_received']).dt.date
        orders_df['date_ordered'] = pd.to_datetime(orders_df['date_ordered']).dt.date
        orders_df['date_shipped'] = pd.to_datetime(orders_df['date_shipped']).dt.date
        stock_info_df['date'] = stock_info_df['date'].dt.date

        #begin data merge of order and po into stock df
        common.logger.debug('Begin Manipulation and Merging of Order and PO info into Stock DF')
        
        stock_info_df['e_date'] = stock_info_df.apply(lambda row: get_earliest_date(row,df=stock_info_df),axis=1) #get earliest inventory date for each sku_id - uses simply apply to find minimum on a SKU basis
        base_available_to_sell_df = get_base_available_to_sell(stock_info_df[['ean','date','available_to_sell']]).rename('base_available_to_sell') #get base_data for start of season calcs - returns DF with 'ean' as index and 'base_available_to_sell' column 

        stock_info_df.set_index('ean',inplace=True) #set stock DF with 'ean' as index in preparation for join
        stock_info_df = stock_info_df.join(base_available_to_sell_df) #do join on 'ean'
        stock_info_df.reset_index(inplace=True) #reset index 
        
        common.logger.debug('Base data merge complete - starting collection of po and orders DFs')
        stock_info_df = stock_info_df[(stock_info_df['date'] == latest_date)].copy()#get rid of all stock rows that are before latest date - don't need them anymore
        stock_info_df['url_markdown'] = stock_info_df['url'].map(lambda a : "[![Image Not Available](" + str(a) + ")](https://aemery.com)")  #get correctly formatted markdown to display images in data_table

        #get  in additional purchase information with 'ean' as index of type string
        additional_purchases_df = get_additonal_purchases(po_df).rename(columns={'result':'additional_purchases'})
        additional_purchases_df.index = additional_purchases_df.index.astype(str)

        #get online and wholesale last week orders with 'ean' as index of type string
        online_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']=='eCommerce']).rename(columns={'result':'online_orders_prev_week'})#.rename('online_orders_prev_week')
        online_orders_prev_week_df.index = online_orders_prev_week_df.index.astype(str)
        wholesale_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']!='eCommerce']).rename(columns={'result':'wholesale_orders_prev_week'})#.rename('wholesale_orders_prev_week')
        wholesale_orders_prev_week_df.index = wholesale_orders_prev_week_df.index.astype(str)

        #get online and wholesale since start orders with 'ean' as index of type string
        online_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']=='eCommerce'])).rename(columns={'result':'online_orders_since_start'})#.rename('online_orders_since_start')
        online_orders_since_start_df.index = online_orders_since_start_df.index.astype(str)
        wholesale_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']!='eCommerce'])).rename(columns={'result':'wholesale_orders_since_start'})#.rename('wholesale_orders_since_start')  
        wholesale_orders_since_start_df.index = wholesale_orders_since_start_df.index.astype(str)

        common.logger.debug('Finished collection of po and order info - starting merge of PO and order info into Stock DF')
        stock_info_df.set_index('ean',inplace=True)#preparation for merge on 'ean' as index of type string
        stock_info_df.index = stock_info_df.index.astype(str)
        
        #do the joins (ie. merges) of po and orders info into Stock DF
        stock_info_df = stock_info_df.join(additional_purchases_df)
        stock_info_df = stock_info_df.join(online_orders_prev_week_df)
        stock_info_df = stock_info_df.join(wholesale_orders_prev_week_df)
        stock_info_df = stock_info_df.join(online_orders_since_start_df)
        stock_info_df = stock_info_df.join(wholesale_orders_since_start_df)

        #make sure any non joined info NaNs are replaced by zeroes for calcs to work
        stock_info_df['additional_purchases'].fillna(0,inplace=True)
        stock_info_df['online_orders_prev_week'].fillna(0,inplace=True)
        stock_info_df['wholesale_orders_prev_week'].fillna(0,inplace=True)
        stock_info_df['online_orders_since_start'].fillna(0,inplace=True)
        stock_info_df['wholesale_orders_since_start'].fillna(0,inplace=True)
        stock_info_df.reset_index(inplace=True)
        
        common.logger.debug('start vectored operations for calculating extra columns')
        stock_info_df['base_stock'] = stock_info_df['base_available_to_sell'] + stock_info_df['additional_purchases']
        stock_info_df['online_revenue_since_start'] = stock_info_df['online_orders_since_start'] * stock_info_df['price_eCommerce_mrsp']
        stock_info_df['wholesale_revenue_since_start'] = stock_info_df['wholesale_orders_since_start'] * stock_info_df['price_eCommerce_mrsp']
        common.logger.debug('finished vectored operations - data manipulation and merge complete')

        
        #from here all about presenting the data table

        stock_info_df = stock_info_df[['url_markdown','e_date','season','p_name','color','size','sku_id','base_available_to_sell','available_to_sell','base_stock','online_orders_prev_week', \
                             'online_orders_since_start','online_revenue_since_start','wholesale_orders_prev_week','wholesale_orders_since_start','wholesale_revenue_since_start']]

        col_title_mapping = {'url_markdown':'Image','e_date':'Earliest Data','season':'Season(s)','p_name':'Product','color':'Colour','size':'Size','category':'Category','sub_category':'Sub Category','sku_id':'SKU', \
                             'in_stock':'In Stock','base_available_to_sell':'Seasonal Units Ordered','available_to_sell':'Available To Sell','available_to_sell_from_stock':'Available To Sell From Stock', \
                             'additional_purchases': 'Additional Purchases','base_stock' : 'Base Stock','online_orders_prev_week': 'Online Units Last Week','wholesale_orders_prev_week' : 'Wholesale Units Last Week', \
                             'online_orders_since_start' : 'Online Units Since Start','wholesale_orders_since_start':'Wholesale Units Since Start','online_revenue_since_start':'Online $$$ Since Start', \
                             'wholesale_revenue_since_start':'Wholesale $$$ Since Start','online_pc_since_start':'Online %','wholesale_pc_since_start':'Wholesale %','seasonal_sell_through_pc':'Seasonal Sell Through %',\
                             'daily_sell_rate':'Daily Sell Rate','estimated_sell_out_weeks':'Estimated Weeks to Sell Out'}


        display_columns = stock_info_df.columns.tolist()
        curr_display_columns = display_columns

        product_option_list = sorted(stock_info_df['p_name'].unique().tolist())
        color_option_list = sorted(stock_info_df['color'].unique().tolist())
        size_option_list = sorted(stock_info_df['size'].unique().tolist())
        season_option_list = []
        
        for ss in stock_info_df['season'].to_list():
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
                            html.P("Start Date - earliest is " + earliest_date.strftime('%d-%m-%Y')),
                            html.Div([
                                dcc.DatePickerSingle(
                                    id='start_date_picker',
                                    min_date_allowed = earliest_date,
                                    max_date_allowed = latest_date,
                                    initial_visible_month = earliest_date,
                                    date = earliest_date,
                                    display_format = 'D-M-Y'
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
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.P("End Season Date"),
                            html.Div([
                                dcc.DatePickerSingle(
                                    id='end_date_picker',
                                    min_date_allowed = (aest_now + timedelta(days=1)).date(),
                                    initial_visible_month = aest_now.date(),
                                    date = end_season_date,
                                    display_format = 'D-M-Y'
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
                            columns=[{"name": col_title_mapping[i], "id": i, 'presentation':'markdown'} if ('markdown' in i) else {"name": col_title_mapping[i], "id": i} for i in stock_info_df.columns],
                            data=stock_info_df[curr_display_columns].to_dict("records"),
                            style_cell_conditional = [
                                {
                                    'if':{'column_id':i},
                                    'textAlign':'center'
                                } for i in ['url_markdown']
                            ],
                            style_cell={'maxWidth':'50px','whiteSpace':'normal'},
                            style_header={'textAlign':'center'},
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
    Input('start_date_picker', 'date'))
def update_output(date_value):
    global base_start_date
    if date_value is not None:
        base_start_date = date.fromisoformat(date_value)
    return None

@dash_app.callback(
    Input('end_date_picker', 'date'))
def update_output(date_value):
    global end_season_date
    if date_value is not None:
        end_season_date = date.fromisoformat(date_value)
    return None

@dash_app.callback(
    Output('product_option', 'options'),
    Input('season_option', 'value')
)
def set_dropdown_options(season):
    global stock_info_df
    dff = stock_info_df.copy()
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
    global stock_info_df
    dff = stock_info_df.copy()
    if product:
        dff = dff[dff['p_name'].isin(product)]
    return [{'label':x,'value':x} for x in dff['color'].unique()]

@dash_app.callback(
    Output('size_option', 'options'),
    [Input('product_option', 'value'),
    Input('color_option','value')]
)
def set_dropdown_options(product,color):
    global stock_info_df
    dff = stock_info_df.copy()
    if product:
        dff = dff[dff['p_name'].isin(product)]
    if color:
        dff = dff[dff['color'].isin(color)]
    return [{'label':x,'value':x} for x in dff['size'].unique()]


def add_additional_calcs(df):
    global latest_date,base_start_date
    
    #new columns and the column they come after
    calc_cols_positions = {'online_pc_since_start':'online_revenue_since_start','wholesale_pc_since_start':'wholesale_revenue_since_start','seasonal_sell_through_pc':'wholesale_pc_since_start',\
                           'daily_sell_rate':'seasonal_sell_through_pc','estimated_sell_out_weeks':'daily_sell_rate'}
    calc_cols = list(calc_cols_positions.keys())
    #stock_info_df = stock_info_df[['url_markdown','e_date','season','p_name','color','size','sku_id','base_available_to_sell','available_to_sell','base_stock','online_orders_prev_week', \
    #                         'online_orders_since_start','online_revenue_since_start','wholesale_orders_prev_week','wholesale_orders_since_start','wholesale_revenue_since_start']]

    df['online_pc_since_start'] = df['online_orders_since_start'] / (df['online_orders_since_start'] + df['wholesale_orders_since_start']) * 100
    df['wholesale_pc_since_start'] = df['wholesale_orders_since_start'] / (df['online_orders_since_start'] + df['wholesale_orders_since_start']) * 100
    df['seasonal_sell_through_pc'] = (df['online_orders_since_start'] + df['wholesale_orders_since_start']) / df['base_stock'] * 100
    df['daily_sell_rate'] = (df['online_orders_since_start'] + df['wholesale_orders_since_start']) / (latest_date - base_start_date).days
    df['estimated_sell_out_weeks'] = df['available_to_sell'] / df['daily_sell_rate']
    
    df[calc_cols] = df[calc_cols].replace([np.inf,-np.inf],0)


    #loop to insert new cols into DF
    new_cols = []
    old_cols = df.columns.tolist()
    i = 0
    new_found = False
    col = old_cols[i]
    new_cols.append(col)
    while i < len(old_cols):
        for k,v in calc_cols_positions.items():
            if (k not in new_cols) and (v == col):
                new_cols.append(k)
                col = k
                new_found = True
                break
        if not new_found:
            i += 1
            if i < len(old_cols):
                col = old_cols[i]
                new_cols.append(col)
        else:
            new_found=False

    common.logger.info('New Cols:' + str(new_cols))

    return new_cols, df[new_cols]
        
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
    global stock_info_df,display_columns,curr_display_columns,latest_date,earliest_date


    try:
        dff = stock_info_df.copy()
        group_list = []
        sum_list = ['base_available_to_sell','available_to_sell','base_stock','online_orders_last_week','wholesale_orders_last_week','online_orders_since_start',\
                    'wholesale_orders_since_start','online_revenue_since_start','wholesale_revenue_since_start']
        present_list = display_columns.copy()
        
        if not v_season or v_season == 'All':
            v_seasons = season_option_list
        else:
            v_seasons = []
            for ss in v_season:
                for s in ss.split(','):
                    if s not in v_seasons:
                        v_seasons.append(s)
        dff = dff[(dff['season'].str.contains('|'.join(v_seasons)))]

        if v_product : 
            dff = dff[dff['p_name'].isin(v_product)]
        if v_color :
            dff = dff[dff['color'].isin(v_color)]
        if v_size :
            dff = dff[dff['size'].isin(v_size)]
        

        if not v_size:
            group_list.append('color')
            present_list.remove('size')
            if 'sku_id' in present_list:
                present_list.remove('sku_id')
        if not v_color:
            group_list.append('p_name')
            present_list.remove('color')
        if not v_product:
            group_list.append('season')
            #present_list.remove('p_name')  #don't remove product as should always be displayed  ########

        agg_dict = {}
        for x in present_list:
            if x not in group_list:
                if x in sum_list:
                    agg_dict[x] = 'sum'
                else:
                    agg_dict[x] = 'first'

        if group_list:
            df_grouped = dff.groupby(group_list).agg(agg_dict).reset_index()
        else:
            df_grouped = dff

        curr_display_columns, df_display = add_additional_calcs(df_grouped[present_list])
        return df_display[curr_display_columns].to_dict("records")
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

dash_app.layout = serve_layout
       