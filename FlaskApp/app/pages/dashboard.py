import os
import pandas as pd
import dash
from dash import html, dcc, callback, dash_table, clientside_callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash import dash_table
from dash.dash_table import DataTable, FormatTemplate
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
import uuid


from FlaskApp.app import app
import FlaskApp.app.common as common
from FlaskApp.app.data_store import get_data_from_data_store, get_data_from_globals

#external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']

customer = 'aemery'

utc_zone = tz.tzutc()
to_zone = tz.gettz('Australia/Melbourne')

CACHE_CONFIG = {
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 86400  #extend cache across time period of one day.
}
cache = Cache()
cache.init_app(app, config=CACHE_CONFIG)

dash.register_page(__name__,path='/')
#dash_app = dash.Dash(server=app,use_pages=True,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/") #previousy 'routes_pathname_prefix'

#common.logger.info (str(app.view_functions))

#for view_func in app.view_functions:
#    if view_func.startswith(dash_app.config['routes_pathname_prefix']):
#        app.view_functions[view_func] = login_required(app.view_functions[view_func])

def flush_cache():
    with app.app_context():
        cache.clear()

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

    return df.assign(result=np.where((df['date_shipped']>=start_date)&(df['date_shipped']<=end_of_previous_week),df['qty_shipped'],0)).groupby('ean').agg({'result':'sum'})

def get_orders_since_start(df,base_start_date):
    #global base_start_date
    return df.assign(result=np.where(df['date_shipped']>=base_start_date,df['qty_shipped'],0)).groupby('ean').agg({'result':'sum'})

def get_additonal_purchases(df,base_start_date):
    #global base_start_date
    return df.assign(result=np.where((df['date_received']>=base_start_date) & (~df['po_number'].str.contains('CRN')),df['qty_received'],0)).groupby('ean').agg({'result':'sum'})

def get_returns(df,base_start_date):
    #global base_start_date
    return df.assign(result=np.where((df['date_received']>=base_start_date) & (df['po_number'].str.contains('CRN')),df['qty_received'],0)).groupby('ean').agg({'result':'sum'})

        
def process_data(base_start_date): #process data based on base_start_date --> need to call it whenever base_start_date changes
    global stock_info_df,orders_df,po_df
    global latest_date,earliest_date,default_end_season_date
    global start_of_previous_week,end_of_previous_week
    
    try:
        stock_info_df,orders_df,po_df, latest_date,earliest_date, default_end_season_date, start_of_previous_week,end_of_previous_week = get_data_from_globals()

        #tb = traceback.format_stack()
        #common.logger.info('Traceback for process_data :' + '\n' + str(tb) + '\n' + 'Base Start Date ' + str(base_start_date) + '\n' + 'Base Start Date Type' + str(type(base_start_date)))
        #common.logger.info('Base Start Date Type' + str(type(base_start_date)))
        #begin data merge of order and po into stock df
        common.logger.debug('Begin Manipulation and Merging of Order and PO info into Stock DF')
                
        #common.logger.info('New process data :' + str(uuid.uuid4()) + ' ------- ' + str(datetime.now()) +'\n' + str(type(base_start_date)) + '\n' +  str(base_start_date))
        base_stock_info_df = stock_info_df.copy()
        
        base_available_to_sell_df = get_base_available_to_sell(stock_info_df[['ean','date','available_to_sell']],base_start_date).rename('base_available_to_sell') #get base_data for start of season calcs - returns DF with 'ean' as index and 'base_available_to_sell' column 

        base_stock_info_df.set_index('ean',inplace=True) #set stock DF with 'ean' as index in preparation for join
        base_stock_info_df = base_stock_info_df.join(base_available_to_sell_df) #do join on 'ean'
        base_stock_info_df.reset_index(inplace=True) #reset index 
        
        common.logger.debug('Base data merge complete - starting collection of po and orders DFs')

        base_stock_info_df = base_stock_info_df[(base_stock_info_df['date'] == latest_date)].copy()#get rid of all stock rows that are before latest date - don't need them anymore
        base_stock_info_df['url_markdown'] = base_stock_info_df['url'].map(lambda a : "[![Image Not Available](" + str(a) + ")](https://aemery.com)")  #get correctly formatted markdown to display images in data_table
        common.logger.debug(str(base_stock_info_df[['ean','category','sub_category','in_stock','available_to_sell','base_available_to_sell']]))
        #base_stock_info_df.to_csv('/Users/Mac/Downloads/stock_info.csv')
        #base_available_to_sell_df.to_csv('/Users/Mac/Downloads/base_available_to_sell.csv')
        #get additional purchase information with 'ean' as index of type string
        additional_purchases_df = get_additonal_purchases(po_df,base_start_date).rename(columns={'result':'additional_purchases'})
        additional_purchases_df.index = additional_purchases_df.index.astype(str)
        #additional_purchases_df.to_csv('/Users/Mac/Downloads/additional_purchases.csv')

        #get returns information with 'ean' as index of type string
        returns_df = get_returns(po_df,base_start_date).rename(columns={'result':'returns'})
        returns_df.index = returns_df.index.astype(str)

        #get online and wholesale last week orders with 'ean' as index of type string
        online_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']=='eCommerce'],base_start_date).rename(columns={'result':'online_orders_prev_week'})#.rename('online_orders_prev_week')
        online_orders_prev_week_df.index = online_orders_prev_week_df.index.astype(str)
        wholesale_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']!='eCommerce'],base_start_date).rename(columns={'result':'wholesale_orders_prev_week'})#.rename('wholesale_orders_prev_week')
        wholesale_orders_prev_week_df.index = wholesale_orders_prev_week_df.index.astype(str)
        common.logger.debug(str(online_orders_prev_week_df))
        #get online and wholesale since start orders with 'ean' as index of type string
        online_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']=='eCommerce']),base_start_date).rename(columns={'result':'online_orders_since_start'})#.rename('online_orders_since_start')
        online_orders_since_start_df.index = online_orders_since_start_df.index.astype(str)
        wholesale_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']!='eCommerce']),base_start_date).rename(columns={'result':'wholesale_orders_since_start'})#.rename('wholesale_orders_since_start')  
        wholesale_orders_since_start_df.index = wholesale_orders_since_start_df.index.astype(str)

        common.logger.debug('Finished collection of po and order info - starting merge of PO and order info into Stock DF')
        common.logger.debug(str(base_stock_info_df.columns))
        common.logger.debug(str(base_stock_info_df[['category','sub_category','in_stock','available_to_sell','base_available_to_sell']]))
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
        base_stock_info_df['additional_purchases'] = base_stock_info_df['additional_purchases'].fillna(0)
        base_stock_info_df['returns'] = base_stock_info_df['returns'].fillna(0)
        base_stock_info_df['online_orders_prev_week'] = base_stock_info_df['online_orders_prev_week'].fillna(0)
        base_stock_info_df['wholesale_orders_prev_week'] = base_stock_info_df['wholesale_orders_prev_week'].fillna(0)
        base_stock_info_df['online_orders_since_start'] = base_stock_info_df['online_orders_since_start'].fillna(0)
        base_stock_info_df['wholesale_orders_since_start'] = base_stock_info_df['wholesale_orders_since_start'].fillna(0)
        base_stock_info_df.reset_index(inplace=True)
        
        common.logger.debug('start vectored operations for calculating extra columns')
        common.logger.debug(str(base_stock_info_df))
        #base_stock_info_df.to_csv('/Users/Mac/Downloads/stock_info_after.csv')
        base_stock_info_df['base_stock'] = base_stock_info_df['base_available_to_sell'] + base_stock_info_df['additional_purchases'] + base_stock_info_df['returns']
        base_stock_info_df['online_revenue_since_start'] = base_stock_info_df['online_orders_since_start'] * base_stock_info_df['price_eCommerce_mrsp']
        base_stock_info_df['wholesale_revenue_since_start'] = base_stock_info_df['wholesale_orders_since_start'] * base_stock_info_df['price_eCommerce_mrsp']

        base_stock_info_df['online_pc_since_start'] = base_stock_info_df['online_orders_since_start'] / (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start'])
        base_stock_info_df['wholesale_pc_since_start'] = base_stock_info_df['wholesale_orders_since_start'] / (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start'])
        base_stock_info_df['seasonal_sell_through_pc'] = (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start']) / base_stock_info_df['base_stock']
        base_stock_info_df['daily_sell_rate'] = (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start']) / (latest_date - base_start_date).days
        base_stock_info_df['return_rate'] = base_stock_info_df['returns'] / (base_stock_info_df['online_orders_since_start'] + base_stock_info_df['wholesale_orders_since_start'])
        base_stock_info_df['estimated_sell_out_weeks'] = base_stock_info_df['available_to_sell'] / base_stock_info_df['daily_sell_rate'] / 7
        
        #fix up any divide by zeroes
        base_stock_info_df[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']] = base_stock_info_df[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']].replace([np.inf,-np.inf],np.nan)
        
        common.logger.debug('finished vectored operations - data manipulation and merge complete')
        common.logger.debug(str(base_stock_info_df))
        #base_stock_info_df.to_csv('/Users/Mac/Downloads/stock_info_end.csv')

        return base_stock_info_df

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))

#def serve_layout(base_stock_info_df,end_season_date):
def layout(**kwargs):
    global stock_info_df,orders_df,po_df
    global latest_date,earliest_date,default_end_season_date
    global start_of_previous_week,end_of_previous_week

    global display_columns
    #global base_stock_info_df,display_stock_info_df
    #global product_option_list,color_option_list,size_option_list,season_option_list    
    
    try:
        common.logger.debug('New dashboard layout ' + str(uuid.uuid4()) + ' ------- ' + str(datetime.now()))
        stock_info_df,orders_df,po_df, latest_date,earliest_date, default_end_season_date, start_of_previous_week,end_of_previous_week = get_data_from_globals()

        base_stock_info_df = global_store(earliest_date)
        #from here all about presenting the data table

        display_columns = ['url_markdown','season','category','sub_category','p_name','color','size','base_available_to_sell','returns','additional_purchases','base_stock','available_to_sell','online_orders_prev_week', \
                           'online_orders_since_start','online_pc_since_start','online_revenue_since_start','wholesale_orders_prev_week','wholesale_orders_since_start','wholesale_pc_since_start','wholesale_revenue_since_start',\
                           'seasonal_sell_through_pc','daily_sell_rate','return_rate','estimated_sell_out_weeks']

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
        money = FormatTemplate.money(0)
        percentage = FormatTemplate.percentage(0)
        fixed = Format(precision=2, scheme=Scheme.fixed)

        col_title_mapping = {
            'url_markdown':{'id':'url_markdown','name':'Image','presentation':'markdown'},
            #'e_date':{'id':'e_date','name':'Earliest Data'},
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
            'returns':{'id':'returns','name':' Returns Since Start'},
            'additional_purchases':{'id':'additional_purchases','name':'Purchases Since Start'},
            #'base_stock':{'id':'base_stock','name':'Base Stock'},
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
            'return_rate':{'id':'return_rate','name':'Return Rate %','type':'numeric','format':percentage},
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

        presentation_shortcuts = [
            'Top 10 Sellers',
            'Bottom 10 Sellers',
        ]
        common.logger.debug('Finished dashboard layout ' + str(uuid.uuid4()) + ' ------- ' + str(datetime.now()))
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
                    width={"size":3}
                ),
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Shortcuts"),
                            html.Div([
                                dcc.Dropdown(
                                    id='presentation_shortcut',
                                    options=presentation_shortcuts,
                                    value='Top 10 Sellers',
                                    multi = False,
                                    clearable = True
                                ),
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                    width = {"size":2}
                ),
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.P("START DATE"),
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
                    width = {'size':1,'offset':4}
                ),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                dbc.Button("Download CSV", id="btn_csv",color='light',size='lg'),
                                dcc.Download(id="download-dataframe-csv"),
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                dbc.Button("Plot Graphs", id="btn_graphs",color='light',size='lg')#,href = '/dashboard/graphs)
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                ],
                width={"size":1}),
                dbc.Col(
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                dbc.Button("LOGOUT",href='/logout',color='light',size='lg',external_link=True,)
                            ]),
                        ]),
                    ],className="border-0 bg-transparent"),
                    width={"size":1}
                )
            ]),
            dbc.Row([
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
                            row_selectable = 'multi',
                            selected_rows = [],
                            style_cell_conditional = [
                                {
                                    'if':{'column_id':i},
                                    'textAlign':'center'
                                } for i in ['url_markdown']
                            ],
                            style_cell={'maxWidth':'50px','minWidth':'50px','whiteSpace':'normal'},
                            style_header={'textAlign':'center','fontsize':'8px','font-weight':'bold'},
                            css=[dict(selector= "p", rule= "margin: 0; text-align: center"),  #used to centre images 
                                     {"selector": ".show-hide", "rule": "display: none"}],  #used to hide toggle columns selector  
                            sort_action = 'native',
                        )
                )
                #, xs=12, md=12, lg=6) some sizing parameters
                #    ]),
                #]),
            ]),
            dcc.Store(id='signal'),
            dcc.Store(id='download'),
            dcc.Store(id='graph-rows'),
            html.Div(id='dummy-div')
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

@callback(Output('signal','data'),
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
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))

@callback(
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
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))


@callback(
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


@callback(
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
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))

@callback(
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
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))

@callback(
    Output("download-dataframe-csv", "data"),
    Input('btn_csv','n_clicks'),
    State("download", "data"),
    prevent_initial_call=True,
)
def func(n_clicks,df_dict):
    return dcc.send_data_frame(pd.DataFrame.from_dict(df_dict).to_csv, "data.csv", index=False)

def add_additional_calcs(df,base_start_date):
    global latest_date
    dff = df.copy()
    if type(base_start_date) == str:
        base_start_date = datetime.strptime(base_start_date,'%Y-%m-%d').date()

    dff['online_pc_since_start'] = dff['online_orders_since_start'] / (dff['online_orders_since_start'] + dff['wholesale_orders_since_start'])
    dff['wholesale_pc_since_start'] = dff['wholesale_orders_since_start'] / (dff['online_orders_since_start'] + dff['wholesale_orders_since_start']) 
    dff['seasonal_sell_through_pc'] = (dff['online_orders_since_start'] + dff['wholesale_orders_since_start']) / dff['base_stock']
    dff['daily_sell_rate'] = (dff['online_orders_since_start'] + dff['wholesale_orders_since_start']) / (latest_date - base_start_date).days
    dff['return_rate'] = dff['returns'] / (dff['online_orders_since_start'] + dff['wholesale_orders_since_start'])
    dff['estimated_sell_out_weeks'] = dff['available_to_sell'] / dff['daily_sell_rate'] / 7
    
    dff[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']] = \
        dff[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']].replace([np.inf,-np.inf],np.nan)
    #dff.to_csv('/Users/Mac/Downloads/display.csv') 
    return dff
        
@callback (
        [Output('data_table', 'data'),
         Output('data_table', 'hidden_columns'),
         Output('download', 'data'),
         Output('data_table','selected_rows')],
        [Input('season_option','value'),
         Input('category_option','value'),
         Input('sub_cat_option','value'),
         Input('product_option', 'value'),
         Input('color_option','value'),
         Input('size_option','value'),
         Input('presentation_shortcut','value'),
         Input('signal','data')],
        running=[(Output("dd-output-container","children"),'Data Being Updated.....Please Wait', 'Data Update Complete'),
                 (Output("dd-output-container","style"),{'backgroundColor':'red','color':'white'},{'backgroundColor':'white','color':'black'})]
)
def update_table(v_season,v_category,v_sub_cat,v_product,v_color,v_size,v_shortcut,v_base_start_date):
    global display_columns

    try:
        if v_base_start_date:
            dff = global_store(v_base_start_date)[display_columns].copy()
            product_option_list = sorted(dff['p_name'].unique().tolist())
            color_option_list = sorted(dff['color'].unique().tolist())
            size_option_list = sorted(dff['size'].unique().tolist())
            season_option_list = []
            
            for ss in dff['season'].to_list():
                for s in ss.split(','):
                    if s not in season_option_list:
                        season_option_list.append(s)
            season_option_list.sort()

            group_list = []
            sum_list = ['base_available_to_sell','available_to_sell','additional_purchases','returns','base_stock','online_orders_prev_week','wholesale_orders_prev_week','online_orders_since_start',\
                        'wholesale_orders_since_start','online_revenue_since_start','wholesale_revenue_since_start']
            present_columns = display_columns.copy()
            
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
            if 'All' in v_size:
                v_size = dff['size'].unique().tolist()            
            
            if v_color :
                dff = dff[dff['color'].isin(v_color)]
            if v_size :
                #common.logger.info('size choice:' + str(v_size) + '\n' + str(dff['size'].unique().tolist()))
                dff = dff[dff['size'].isin(v_size)]
            
            
            group_list.append('season') #always group season
            group_list.append('p_name') #always group products
            
            if not v_color:
                present_columns.remove('color')
                if 'sku_id' in present_columns:
                    present_columns.remove('sku_id')
            else:
                group_list.append('color')
            
            if not v_size:
                if 'color' in present_columns:
                    if 'color' not in group_list:
                        group_list.append('color')
                present_columns.remove('size')
                if 'sku_id' in present_columns:
                    present_columns.remove('sku_id')
            else:
                group_list.append('size') #need this to make sure all sizes selected are displayed
            
            #common.logger.info('v_season' + str(v_season) + '\nv_product: ' + str(v_product) + '\nv_color: ' + str(v_color) + '\n' + \
            #                   'v_size: ' + str(v_size) + '\nGroup List: ' + str(group_list) + '\nPresent List: ' + str(present_columns))

            agg_dict = {}
            for x in present_columns:
                if x not in group_list:
                    if x in sum_list:
                        agg_dict[x] = 'sum'
                    else:
                        agg_dict[x] = 'first'
            #common.logger.info(str(group_list) + '\n' + str(present_columns))
            if group_list:
                df_grouped = dff.groupby(group_list).agg(agg_dict).reset_index()
            else:
                df_grouped = dff

            hidden_columns = list(set(display_columns) - set(present_columns))

            df_display = add_additional_calcs(df_grouped[present_columns],v_base_start_date)

            if v_shortcut == 'Top 10 Sellers':
                df_display = df_display[df_display['seasonal_sell_through_pc']>0].sort_values('seasonal_sell_through_pc',ascending=False,ignore_index=True).head(10)
            elif v_shortcut == 'Bottom 10 Sellers':
                df_display = df_display[df_display['seasonal_sell_through_pc']>0].sort_values('seasonal_sell_through_pc',ascending=True,ignore_index=True).head(10)

            df_download = df_display.drop('url_markdown',axis=1).copy()

            #debug_csv_file_data = df_grouped.to_csv()
            #common.store_dropbox_unicode(customer,debug_csv_file_data,os.path.join(data_store_folder,'debug_group' + str(group_list) + '.csv'))
            return df_display.to_dict("records"), hidden_columns, df_download.to_dict("records"), []
        else:
            return None
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

clientside_callback(
    """
    function(n_clicks,rows,data) {
        const send_data = rows.map(index => data[index]);
        const sendjsonString = JSON.stringify(send_data)
        const url = `https://api-test.mclarenwilliams.com.au/dashboard/graphs?data=${sendjsonString}`;
        window.open(url,'_blank');
    }
    """,
    Output('dummy-div', 'children'),
    Input('btn_graphs', 'n_clicks'),
    State('data_table', 'selected_rows'),
    State('download', 'data'),
    prevent_initial_call=True #
)

@callback (
    Output('graph-rows','data'),
    Input('data_table','selected_rows'),
    State('download','data')

)
def updated_selected_rows(v_rows,display_data):
    try:
        #common.logger.info('Select Rows: ' + str(v_rows) + '\n' + str(display_data))
        if v_rows:
            df = pd.DataFrame.from_dict(display_data).iloc[v_rows]
            df_cols = df.columns.tolist()
            if 'color' in df_cols:
                if 'size' in df_cols:
                    dff = df[['p_name','color','size']]
                else:
                    dff = df[['p_name','color']]
            elif 'size' in df_cols:
                dff = df[['p_name','size']]
            else:
                dff = df['p_name']
                return dff.to_list()
            #common.logger.info('Select Rows 2: ' + str(v_rows) + '\n' + str(dff.head()))
            return dff.to_dict('records')
        else:
            return None
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

flush_cache()


#dash_app.layout = partial(serve_layout, process_data(earliest_date),default_end_season_date)
       