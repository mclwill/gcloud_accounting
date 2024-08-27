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
from dash_table import DataTable, FormatTemplate
from dash.exceptions import PreventUpdate
from dash.dash_table.Format import Format, Scheme, Trim
import plotly.express as px
import traceback
from datetime import datetime, date, time, timedelta
from dateutil import tz
import numpy as np
from flask_caching import Cache
#import redis
from functools import partial

import FlaskApp.app.common as common

external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']

customer = 'aemery'

utc_zone = tz.tzutc()
to_zone = tz.gettz('Australia/Melbourne')

data_store_folder = common.data_store[customer]
stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
orders_file_path = os.path.join(data_store_folder,'data_orders.csv')
po_file_path = os.path.join(data_store_folder,'data_po.csv')

CACHE_CONFIG = {
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 86400  #extend cache across time period of one day.
}
cache = Cache()
cache.init_app(app, config=CACHE_CONFIG)

dash_app = dash.Dash(server=app,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/") #previousy 'routes_pathname_prefix'

#common.logger.info (str(app.view_functions))

for view_func in app.view_functions:
    if view_func.startswith(dash_app.config['routes_pathname_prefix']):
        app.view_functions[view_func] = login_required(app.view_functions[view_func])

def flush_cache():
    with app.app_context():
        cache.clear()

def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)

def get_start_of_previous_week(date_value):
    weekday = date_value.weekday()
    monday_delta = timedelta(days=weekday,weeks=1)
    return date_value - monday_delta

def get_earliest_date(row,df):
    return df['date'][df['sku_id'] == row['sku_id']].min()

def get_base_available_to_sell(df,base_start_date):
    #global base_start_date
    #common.logger.info(str(df[(df['sku_id'] == row['sku_id'])&(df['date']==base_start_date)].loc[:,'available_to_sell'].values))
    return_df = df[['ean','available_to_sell']][(df['date']==base_start_date)]
    return_df.rename({'available_to_sell':'base_available_to_sell'},inplace=True,axis=1)
    return_df.set_index('ean',inplace=True)
    return return_df['base_available_to_sell']

def get_last_week_orders(df,base_start_date):
    global start_of_previous_week,end_of_previous_week#base_start_date
    if start_of_previous_week < base_start_date :
        start_date = base_start_date
    else:
        start_date = start_of_previous_week

    return df.assign(result=np.where((df['date_shipped']>=start_date)&(df['date_shipped']<=end_of_previous_week),df['qty_shipped'],0)).groupby('ean').agg({'result':sum})

def get_orders_since_start(df,base_start_date):
    #global base_start_date
    return df.assign(result=np.where(df['date_shipped']>=base_start_date,df['qty_shipped'],0)).groupby('ean').agg({'result':sum})

def get_additonal_purchases(df,base_start_date):
    #global base_start_date
    return df.assign(result=np.where((df['date_received']>=base_start_date) & (~df['po_number'].str.contains('CRN')),df['qty_received'],0)).groupby('ean').agg({'result':sum})

def get_returns(df,base_start_date):
    #global base_start_date
    return df.assign(result=np.where((df['date_received']>=base_start_date) & (df['po_number'].str.contains('CRN')),df['qty_received'],0)).groupby('ean').agg({'result':sum})

def get_data_from_data_store():

    global stock_info_df,orders_df,po_df
    global latest_date,earliest_date,aest_now,default_end_season_date
    global start_of_previous_week,end_of_previous_week
    

    try:
        #collect data in serve_layout so that latest is retrieved from data_store

        flush_cache() #ensure cache is flush before getting data from data store to make sure it doesn't get too big.

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
        #base_start_date = earliest_date #establish base date for calculating percentages etc (ie. start of season) as earliest date ---> need to modify this when this can be set through the dashboard using 'Start Date'
        default_end_season_date = last_day_of_month(aest_now.date()) #default data as end of this month
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

        stock_info_df['e_date'] = stock_info_df.apply(lambda row: get_earliest_date(row,df=stock_info_df),axis=1) #get earliest inventory date for each sku_id - uses simply apply to find minimum on a SKU basis

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))     

        
def process_data(base_start_date): #process data based on base_start_date --> need to call it whenever base_start_date changes
    global stock_info_df,orders_df,po_df
    global latest_date,earliest_date
    global start_of_previous_week,end_of_previous_week
    
    try:
        #common.logger.info('Base Start Date Type' + str(type(base_start_date)))
        #begin data merge of order and po into stock df
        common.logger.debug('Begin Manipulation and Merging of Order and PO info into Stock DF')
        

        base_stock_info_df = stock_info_df.copy()
        
        base_available_to_sell_df = get_base_available_to_sell(stock_info_df[['ean','date','available_to_sell']],base_start_date).rename('base_available_to_sell') #get base_data for start of season calcs - returns DF with 'ean' as index and 'base_available_to_sell' column 

        base_stock_info_df.set_index('ean',inplace=True) #set stock DF with 'ean' as index in preparation for join
        base_stock_info_df = base_stock_info_df.join(base_available_to_sell_df) #do join on 'ean'
        base_stock_info_df.reset_index(inplace=True) #reset index 
        
        common.logger.debug('Base data merge complete - starting collection of po and orders DFs')
        base_stock_info_df = base_stock_info_df[(base_stock_info_df['date'] == latest_date)].copy()#get rid of all stock rows that are before latest date - don't need them anymore
        base_stock_info_df['url_markdown'] = base_stock_info_df['url'].map(lambda a : "[![Image Not Available](" + str(a) + ")](https://aemery.com)")  #get correctly formatted markdown to display images in data_table

        #get additional purchase information with 'ean' as index of type string
        additional_purchases_df = get_additonal_purchases(po_df,base_start_date).rename(columns={'result':'additional_purchases'})
        additional_purchases_df.index = additional_purchases_df.index.astype(str)

        #get returns information with 'ean' as index of type string
        returns_df = get_return_purchases(po_df,base_start_date).rename(columns={'result':'returns'})
        returns_df.index = returns_df.index.astype(str)

        #get online and wholesale last week orders with 'ean' as index of type string
        online_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']=='eCommerce'],base_start_date).rename(columns={'result':'online_orders_prev_week'})#.rename('online_orders_prev_week')
        online_orders_prev_week_df.index = online_orders_prev_week_df.index.astype(str)
        wholesale_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']!='eCommerce'],base_start_date).rename(columns={'result':'wholesale_orders_prev_week'})#.rename('wholesale_orders_prev_week')
        wholesale_orders_prev_week_df.index = wholesale_orders_prev_week_df.index.astype(str)

        #get online and wholesale since start orders with 'ean' as index of type string
        online_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']=='eCommerce']),base_start_date).rename(columns={'result':'online_orders_since_start'})#.rename('online_orders_since_start')
        online_orders_since_start_df.index = online_orders_since_start_df.index.astype(str)
        wholesale_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']!='eCommerce']),base_start_date).rename(columns={'result':'wholesale_orders_since_start'})#.rename('wholesale_orders_since_start')  
        wholesale_orders_since_start_df.index = wholesale_orders_since_start_df.index.astype(str)

        common.logger.debug('Finished collection of po and order info - starting merge of PO and order info into Stock DF')
        base_stock_info_df.set_index('ean',inplace=True)#preparation for merge on 'ean' as index of type string
        base_stock_info_df.index = base_stock_info_df.index.astype(str)
        
        #do the joins (ie. merges) of po and orders info into Stock DF
        base_stock_info_df = base_stock_info_df.join(additional_purchases_df)
        base_stock_info_df = base_stock_info_df.join(returns_df)
        base_stock_info_df = base_stock_info_df.join(online_orders_prev_week_df)
        base_stock_info_df = base_stock_info_df.join(wholesale_orders_prev_week_df)
        base_stock_info_df = base_stock_info_df.join(online_orders_since_start_df)
        base_stock_info_df = base_stock_info_df.join(wholesale_orders_since_start_df)

        #make sure any non joined info NaNs are replaced by zeroes for calcs to work
        base_stock_info_df['additional_purchases'].fillna(0,inplace=True)
        base_stock_info_df['online_orders_prev_week'].fillna(0,inplace=True)
        base_stock_info_df['wholesale_orders_prev_week'].fillna(0,inplace=True)
        base_stock_info_df['online_orders_since_start'].fillna(0,inplace=True)
        base_stock_info_df['wholesale_orders_since_start'].fillna(0,inplace=True)
        base_stock_info_df.reset_index(inplace=True)
        
        common.logger.debug('start vectored operations for calculating extra columns')
        base_stock_info_df['base_stock'] = base_stock_info_df['base_available_to_sell'] + base_stock_info_df['additional_purchases'] + base_stock_info_df['returns']
        base_stock_info_df['online_revenue_since_start'] = base_stock_info_df['online_orders_since_start'] * base_stock_info_df['price_eCommerce_mrsp']
        base_stock_info_df['wholesale_revenue_since_start'] = base_stock_info_df['wholesale_orders_since_start'] * base_stock_info_df['price_eCommerce_mrsp']

        base_stock_info_df['online_pc_since_start'] = base_stock_info_df['online_orders_since_start'] / (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start'])
        base_stock_info_df['wholesale_pc_since_start'] = base_stock_info_df['wholesale_orders_since_start'] / (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start'])
        base_stock_info_df['seasonal_sell_through_pc'] = (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start']) / base_stock_info_df['base_stock']
        base_stock_info_df['daily_sell_rate'] = (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start']) / (latest_date - base_start_date).days
        base_stock_info_df['estimated_sell_out_weeks'] = base_stock_info_df['available_to_sell'] / base_stock_info_df['daily_sell_rate'] / 7
        
        #fix up any divide by zeroes
        base_stock_info_df[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']] = base_stock_info_df[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']].replace([np.inf,-np.inf],np.nan)
        
        common.logger.debug('finished vectored operations - data manipulation and merge complete')

        return base_stock_info_df

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))

def serve_layout(base_stock_info_df,end_season_date):
    global earliest_date, latest_date, display_columns
    #global base_stock_info_df,display_stock_info_df
    #global product_option_list,color_option_list,size_option_list,season_option_list    
    
    try:

        #from here all about presenting the data table

        display_columns = ['url_markdown','season','category','sub_category','p_name','color','size','base_available_to_sell','returns','additional_purchases','base_stock','available_to_sell','online_orders_prev_week', \
                           'online_orders_since_start','online_pc_since_start','online_revenue_since_start','wholesale_orders_prev_week','wholesale_orders_since_start','wholesale_pc_since_start','wholesale_revenue_since_start',\
                           'seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']

        display_stock_info_df = base_stock_info_df[display_columns].copy() #seem to need to take copy

        '''
        col_title_mapping = {'url_markdown':'Image','e_date':'Earliest Data','season':'Season(s)','p_name':'Product','color':'Colour','size':'Size','category':'Category','sub_category':'Sub Category','sku_id':'SKU', \
                             'in_stock':'In Stock','base_available_to_sell':'Seasonal Units Ordered','available_to_sell':'Available To Sell','available_to_sell_from_stock':'Available To Sell From Stock', \
                             'additional_purchases': 'Additional Purchases','base_stock' : 'Base Stock','online_orders_prev_week': 'Online Units Last Week','wholesale_orders_prev_week' : 'Wholesale Units Last Week', \
                             'online_orders_since_start' : 'Online Units Since Start','wholesale_orders_since_start':'Wholesale Units Since Start','online_revenue_since_start':'Online $$$ Since Start', \
                             'wholesale_revenue_since_start':'Wholesale $$$ Since Start','online_pc_since_start':'Online %','wholesale_pc_since_start':'Wholesale %','seasonal_sell_through_pc':'Seasonal Sell Through %',\
                             'daily_sell_rate':'Daily Sell Rate','estimated_sell_out_weeks':'Estimated Weeks to Sell Out'}
        '''
        
        #data table formats and mapping
        money = FormatTemplate.money(2)
        percentage = FormatTemplate.percentage(2)
        fixed = Format(precision=2, scheme=Scheme.fixed)

        col_title_mapping = {
            'url_markdown':{'id':'url_markdown','name':'Image','presentation':'markdown'},
            'e_date':{'id':'e_date','name':'Earliest Data'},
            'season':{'id':'season','name':'Season(s)'},
            'p_name':{'id':'p_name','name':'Product'},
            'color':{'id':'color','name':'Colour'},
            'size':{'id':'size','name':'Size'},
            'category':{'id':'category','name':'Category'},
            'sub_category':{'id':'sub_category','name':'Sub Category'},
            'sku_id':{'id':'sku_id','name':'SKU'},
            'in_stock':{'id':'in_stock','name':'In Stock'},
            'base_available_to_sell':{'id':'base_available_to_sell','name':'Starting Available To Sell'},
            'available_to_sell':{'id':'available_to_sell','name':'Available To Sell'},
            'available_to_sell_from_stock':{'id':'available_to_sell_from_stock','name':'Available To Sell From Stock'},
            'returns':{'id':'returns','name':' Returns'},
            'additional_purchases':{'id':'additional_purchases','name':'Additional Purchases'},
             #'base_stock':{'id':'base_stock','name':'Base Stock','hidden':True},
            'online_orders_prev_week':{'id':'online_orders_prev_week','name':'Online Sales Last Week'},
            'wholesale_orders_prev_week':{'id':'wholesale_orders_prev_week','name':'Wholesale Sales Last Week'},
            'online_orders_since_start':{'id':'online_orders_since_start','name':'Online Sales Since Start'},
            'wholesale_orders_since_start':{'id':'wholesale_orders_since_start','name':'Wholesale Sales Since Start'},
            'online_revenue_since_start':{'id':'online_revenue_since_start','name':'Online $$$ Since Start','type':'numeric','format':money},
            'wholesale_revenue_since_start':{'id':'wholesale_revenue_since_start','name':'Wholesale $$$ Since Start','type':'numeric','format':money},
            'online_pc_since_start':{'id':'online_pc_since_start','name':'Online %','type':'numeric','format':percentage},
            'wholesale_pc_since_start':{'id':'wholesale_pc_since_start','name':'Wholesale %','type':'numeric','format':percentage},
            'seasonal_sell_through_pc':{'id':'seasonal_sell_through_pc','name':'Seasonal Sell Through %','type':'numeric','format':percentage},
            'daily_sell_rate':{'id':'daily_sell_rate','name':'Daily Sell Rate','type':'numeric','format':fixed},
            'estimated_sell_out_weeks':{'id':'estimated_sell_out_weeks','name':'Estimated Weeks to Sell Out','type':'numeric','format':fixed}
        }

        #display_stock_info_df = stock_info_df.copy()
        #display_stock_info_df = display_stock_info_df.reindex(columns = display_stock_info_df.columns.tolist() + ['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks'])
        

        category_option_list = sorted(display_stock_info_df['category'].unique().tolist())
        sub_cat_option_list = sorted(display_stock_info_df['sub_category'].unique().tolist())
        product_option_list = sorted(display_stock_info_df['p_name'].unique().tolist())
        color_option_list = sorted(display_stock_info_df['color'].unique().tolist())
        size_option_list = sorted(display_stock_info_df['size'].unique().tolist())
        season_option_list = []

        #common.logger.info('product list 1 :' + str(product_option_list))
        
        for ss in display_stock_info_df['season'].to_list():
            for s in ss.split(','):
                if s not in season_option_list:
                    season_option_list.append(s)
        season_option_list.sort()

        #debug_csv_file_data = display_stock_info_df.to_csv()
        #common.store_dropbox_unicode(customer,debug_csv_file_data,os.path.join(data_store_folder,'debug.csv'))
        
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
                            html.P("Category"),
                            html.Div([
                                dcc.Dropdown(
                                    id='category_option',
                                    options=category_option_list,
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
                            html.P("Sub Category"),
                            html.Div([
                                dcc.Dropdown(
                                    id='sub_cat_option',
                                    options=sub_cat_option_list,
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
                                    options=['All'] + color_option_list,
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
                                    options=['All'] + size_option_list,
                                    value=[],
                                    #placeholder = 'All',
                                    multi = True,
                                    clearable = True
                                ),
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                ),
                #dbc.Col(
                #    dbc.Card([
                #        dbc.CardBody([
                #            html.P("End Season Date"),
                #            html.Div([
                #                dcc.DatePickerSingle(
                #                    id='end_date_picker',
                #                    min_date_allowed = (aest_now + timedelta(days=1)).date(),
                #                    initial_visible_month = aest_now.date(),
                #                    date = end_season_date,
                #                    display_format = 'D-M-Y'
                #                ),
                #            ]),
                #        ]),
                #    ],className="border-0 bg-transparent"),   
                #),
            ]),
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Div(id='dd-output-container',children='Data Update Complete')
                    ],style={'backgroundColor':'red','color':'white'})
                ]),
            ], align='center'),
            dbc.Row([
                #dbc.Card([
                #    dbc.CardBody([
                dbc.Col(
                        dash_table.DataTable(
                            id='data_table',
                            columns=[col_title_mapping[i] for i in display_columns if i in col_title_mapping.keys()],
                            #columns=[{"name": col_title_mapping[i], "id": i, 'presentation':'markdown'} if ('markdown' in i) else {"name": col_title_mapping[i], "id": i} for i in display_stock_info_df.columns],
                            data=display_stock_info_df.to_dict("records"),
                            style_cell_conditional = [
                                {
                                    'if':{'column_id':i},
                                    'textAlign':'center'
                                } for i in ['url_markdown']
                            ],
                            style_cell={'maxWidth':'50px','minWidth':'50px','whiteSpace':'normal'},
                            style_header={'textAlign':'center','fontsize':'8px','font-weight':'bold'},
                            css=[dict(selector= "p", rule= "margin: 0; text-align: center")],
                            sort_action = 'native',
                        )
                )
                #, xs=12, md=12, lg=6) some sizing parameters
                #    ]),
                #]),
            ]),
            dcc.Store(id='signal')
        ])
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

@cache.memoize()
def global_store(base_start_date):
    try:
        #common.logger.info('Base Start Date in global_store' + str(type(base_start_date)) + '\n' + str(base_start_date))
        if type(base_start_date) == str:
                base_start_date = datetime.strptime(base_start_date,'%Y-%m-%d').date()
        return process_data(base_start_date)
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))

@dash_app.callback(Output('signal','data'),
                  Input('start_date_picker', 'date'),
                  running=[(Output("dd-output-container","children"),'Data Being Updated.....Please Wait', 'Data Update Complete'),
                           (Output("dd-output-container","style"),{'backgroundColor':'red','color':'white'},{'backgroundColor':'white','color':'black'})])
def update_output(date_value):
    #global base_start_date
    #common.logger.info('start Date Picker ' + str(type(date_value)) + '\n' + str(date_value))
    try:
        if date_value is not None:
            #base_start_date = date.fromisoformat(date_value)
            #common.logger.info('Base Start Date in update_output' + str(type(base_start_date)) + '\n' + str(base_start_date))
            #store base_start_date as string
            global_store(date_value)#process_data(base_start_date) #need to reprocess data since 
            #common.logger.info('start Date Picker 2' + str(type(date_value)) + '\n' + str(date_value))
            return date_value
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))

#@dash_app.callback(
#    Input('end_date_picker', 'date'))
#def update_output(date_value):
#    try:
#        #global end_season_date
#        if date_value is not None:
#            end_season_date = date.fromisoformat(date_value)
#        return None
#    except Exception as ex:
#        tb = traceback.format_exc()
#        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))

@dash_app.callback(
    Output('sub_cat_option', 'options'),
    [Input('season_option', 'value'),
     Input('category_option', 'value'),
     Input('signal','data')]
)
def set_dropdown_options(season,category,v_base_start_date):
    try:
        #global display_stock_info_df
        if v_base_start_date:
            dff = global_store(v_base_start_date).copy()
            if season:
                seasons = []
                for ss in season:
                    for s in ss.split(','):
                        if s not in seasons:
                            seasons.append(s)
                dff = dff[dff['season'].str.contains('|'.join(seasons))]
            if category:
                dff = dff[dff['category'].isin(category)]
            return [{'label':x,'value':x} for x in sorted(dff['sub_category'].unique().tolist())]
        else:
            return None
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))


@dash_app.callback(
    Output('product_option', 'options'),
    [Input('season_option', 'value'),
     Input('category_option','value'),
     Input('sub_cat_option','value'),
     Input('signal','data')]
)
def set_dropdown_options(season,category,sub_cat,v_base_start_date):
    try:
        #global display_stock_info_df
        if v_base_start_date:
            dff = global_store(v_base_start_date).copy()
            if season:
                seasons = []
                for ss in season:
                    for s in ss.split(','):
                        if s not in seasons:
                            seasons.append(s)
                dff = dff[dff['season'].str.contains('|'.join(seasons))]
            if category:
                dff = dff[dff['category'].isin(category)]
            if sub_cat:
                dff = dff[dff['sub_category'].isin(sub_cat)]
            return [{'label':x,'value':x} for x in sorted(dff['p_name'].unique().tolist())]
        else:
            return None
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))


@dash_app.callback(
    Output('color_option', 'options'),
    [Input('product_option', 'value'),
    Input('signal','data')]
)
def set_dropdown_options(product,v_base_start_date):
    try:
        #global display_stock_info_df
        if v_base_start_date:
            dff = global_store(v_base_start_date).copy()
            if  product:
                dff = dff[dff['p_name'].isin(product)]
            return [{'label':x,'value':x} for x in (['All'] + sorted(dff['color'].unique().tolist()))]
        else: 
            return None
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))

@dash_app.callback(
    Output('size_option', 'options'),
    [Input('product_option', 'value'),
    Input('color_option','value'),
    Input('signal','data')]
)
def set_dropdown_options(product,color,v_base_start_date):
    #global display_stock_info_df
    try:
        if v_base_start_date:
            dff = global_store(v_base_start_date).copy()
            if product:
                dff = dff[dff['p_name'].isin(product)]
            if color and ('All' not in color):
                dff = dff[dff['color'].isin(color)]
            return [{'label':x,'value':x} for x in (['All'] + sorted(dff['size'].unique().tolist()))]
        else:
            return None
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))


def add_additional_calcs(df,base_start_date):
    global latest_date
    dff = df.copy()
    if type(base_start_date) == str:
        base_start_date = datetime.strptime(base_start_date,'%Y-%m-%d').date()

    dff['online_pc_since_start'] = dff['online_orders_since_start'] / (dff['online_orders_since_start'] + dff['wholesale_orders_since_start'])
    dff['wholesale_pc_since_start'] = dff['wholesale_orders_since_start'] / (dff['online_orders_since_start'] + dff['wholesale_orders_since_start']) 
    dff['seasonal_sell_through_pc'] = (dff['online_orders_since_start'] + dff['wholesale_orders_since_start']) / dff['base_stock']
    dff['daily_sell_rate'] = (dff['online_orders_since_start'] + dff['wholesale_orders_since_start']) / (latest_date - base_start_date).days
    dff['estimated_sell_out_weeks'] = dff['available_to_sell'] / dff['daily_sell_rate'] / 7
    
    dff[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']] = \
        dff[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']].replace([np.inf,-np.inf],np.nan)

    return dff
        
@dash_app.callback (
        Output('data_table', 'data'),
        [Input('season_option','value'),
         Input('category_option','value'),
         Input('sub_cat_option','value'),
         Input('product_option', 'value'),
         Input('color_option','value'),
         Input('size_option','value'),
         Input('signal','data')],
        running=[(Output("dd-output-container","children"),'Data Being Updated.....Please Wait', 'Data Update Complete'),
                 (Output("dd-output-container","style"),{'backgroundColor':'red','color':'white'},{'backgroundColor':'white','color':'black'})]
)
def update_table(v_season,v_category,v_sub_cat,v_product,v_color,v_size,v_base_start_date):
    #global stock_info_df,display_stock_info_df,display_columns,curr_display_columns,latest_date,earliest_date
    #global display_columns,season_option_list, product_option_list, color_option_list, size_option_list
    global display_columns

    try:
        #common.logger.info('Base Start Date Type in update_table' + str(type(v_base_start_date)) + '\n' + str(v_base_start_date))
        #if type(v_base_start_date) == str:
        #    v_base_start_date = datetime.strptime(v_base_start_date,'%Y-%m-%d')
        if v_base_start_date:
            dff = global_store(v_base_start_date)[display_columns].copy()
            #common.logger.info(str(dff.head()))
            #for some reason can't seem to get these from global variables
            #display_columns = dff.columns.tolist()
            product_option_list = sorted(dff['p_name'].unique().tolist())
            color_option_list = sorted(dff['color'].unique().tolist())
            size_option_list = sorted(dff['size'].unique().tolist())
            season_option_list = []

            #common.logger.info('product list 1 :' + str(product_option_list))
            
            for ss in dff['season'].to_list():
                for s in ss.split(','):
                    if s not in season_option_list:
                        season_option_list.append(s)
            season_option_list.sort()

            group_list = []
            sum_list = ['base_available_to_sell','available_to_sell','base_stock','online_orders_prev_week','wholesale_orders_prev_week','online_orders_since_start',\
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

            
            if v_category : 
                dff = dff[dff['category'].isin(v_category)]
            if v_sub_cat : 
                dff = dff[dff['sub_category'].isin(v_sub_cat)]
            if v_product : 
                dff = dff[dff['p_name'].isin(v_product)]
            if 'All' in v_color : 
                v_color = dff['color'].unique().tolist()
            if v_color :
                dff = dff[dff['color'].isin(v_color)]
            if 'All' in v_size:
                v_size = dff['size'].unique().tolist()
            if v_size :
                #common.logger.info('size choice:' + str(v_size) + '\n' + str(dff['size'].unique().tolist()))
                dff = dff[dff['size'].isin(v_size)]
            
            group_list.append('season') #always group season
            group_list.append('p_name') #always group products
            
            if not v_color:
                present_list.remove('color')
                if 'sku_id' in present_list:
                    present_list.remove('sku_id')
            if not v_size:
                if 'color' in present_list:
                    group_list.append('color')
                present_list.remove('size')
                if 'sku_id' in present_list:
                    present_list.remove('sku_id')
            #else:
            #    group_list.append('size')
            
            #common.logger.info('v_season' + str(v_season) + '\nv_product: ' + str(v_product) + '\nv_color: ' + str(v_color) + '\n' + \
            #                   'v_size: ' + str(v_size) + '\nGroup List: ' + str(group_list) + '\nPresent List: ' + str(present_list))

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

            #debug_csv_file_data = df_grouped.to_csv()
            #common.store_dropbox_unicode(customer,debug_csv_file_data,os.path.join(data_store_folder,'debug_group' + str(group_list) + '.csv'))
            return add_additional_calcs(df_grouped[present_list],v_base_start_date).to_dict("records")
        else:
            return None
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

get_data_from_data_store()

dash_app.layout = partial(serve_layout, process_data(earliest_date),default_end_season_date)
       