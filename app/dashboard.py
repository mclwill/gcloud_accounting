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

def get_base_available_to_sell(row,df):
    global base_start_date
    #common.logger.info(str(df[(df['sku_id'] == row['sku_id'])&(df['date']==base_start_date)].loc[:,'available_to_sell'].values))
    return df[(df['sku_id'] == row['sku_id'])&(df['date']==base_start_date)].loc[:,'available_to_sell'].values[0]

def get_extra_data(row,po_df,orders_df):
    global base_start_date,end_season_date,start_of_previous_week,end_of_previous_week
    
    #common.logger.info(str(row))
    row['additional_purchases'] = po_df['qty_received'][(po_df['ean'] == row['ean'])&((po_df['date_received']>base_start_date))].sum()
    row['base_stock'] = row['base_available_to_sell'] + row['additional_purchases']
    
    last_week_mask = (orders_df['date_shipped'] >= start_of_previous_week) & (orders_df['date_shipped'] <= end_of_previous_week)
    row['online_sales_last_week'] = orders_df['qty_shipped'][last_week_mask & (orders_df['channel']=='eCommerce')].sum()
    row['wholesale_sales_last_week'] = orders_df['qty_shipped'][last_week_mask & (orders_df['channel']!='eCommerce')].sum()
    
    online_since_start_mask = (orders_df['date_shipped'] >= base_start_date)&(orders_df['channel']=='eCommerce')
    row['online_sales_since_start'] = orders_df['qty_shipped'][online_since_start_mask].sum()
    wholesale_since_start_mask = (orders_df['date_shipped'] >= base_start_date)&(orders_df['channel']!='eCommerce')
    row['wholesale_sales_since_start'] = orders_df['qty_shipped'][wholesale_since_start_mask].sum()
    row['online_revenue_since_start'] = (orders_df['qty_shipped'][online_since_start_mask] * row['price_eCommerce_mrsp']).sum()
    row['wholesale_revenue_since_start'] = (orders_df['qty_shipped'][wholesale_since_start_mask] * row['price_eCommerce_mrsp']).sum()


    '''
    online_percentage = online_sales_since_start / (online_sales_since_start + wholesale_sales_since_start)
    wholesale_percentage = wholesale_sales_since_start / (online_sales_since_start + wholesale_sales_since_start)

    seasonal_sell_through = (online_sales_since_start + wholesale_sales_since_start) / (base_available_to_sell + additional_purchases)'''

    return row


def serve_layout():
    #global season_stock_info_df
    global stock_info_df,display_columns,latest_date,earliest_date
    global base_start_date,end_season_date,start_of_previous_week,end_of_previous_week
    global product_option_list,color_option_list,size_option_list,season_option_list

    try:
        #collect data in serve_layout so that latest is retrieved from data_store



        aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)

        byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
        if byte_stream:
            stock_info_df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            stock_info_df = pd.DataFrame() #start with empty dataframe

        if stock_info_df.empty:
            return html.Div(html.P('No Stock Data retrieved from Data Store'))

        
        stock_info_df['date'] = pd.to_datetime(stock_info_df['date'])
        latest_date = stock_info_df['date'].max().to_pydatetime().date()
        earliest_date = stock_info_df['date'].min().to_pydatetime().date()
        base_start_date = earliest_date
        end_season_date = last_day_of_month(aest_now.date())
        start_of_previous_week = get_start_of_previous_week(aest_now.date())  #this should be the Monday of the previous week
        end_of_previous_week = start_of_previous_week + timedelta(days=6) #this should be the Sunday of the previous week


        byte_stream = common.read_dropbox_bytestream(customer,orders_file_path)
        if byte_stream:
            orders_df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            orders_df = pd.DataFrame() #start with empty dataframe

        if orders_df.empty:
            return html.Div(html.P('No Orders Data retrieved from Data Store'))

        byte_stream = common.read_dropbox_bytestream(customer,po_file_path)
        if byte_stream:
            po_df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            po_df = pd.DataFrame() #start with empty dataframe

        if po_df.empty:
            return html.Div(html.P('No Purchase Orders Data retrieved from Data Store'))

        po_df['date_received'] = pd.to_datetime(po_df['date_received']).dt.date
        orders_df['date_ordered'] = pd.to_datetime(orders_df['date_ordered']).dt.date
        orders_df['date_shipped'] = pd.to_datetime(orders_df['date_shipped']).dt.date
        
        stock_info_df['date'] = stock_info_df['date'].dt.date

        stock_info_df['e_date'] = stock_info_df.apply(lambda row: get_earliest_date(row,df=stock_info_df),axis=1) #get earliest inventory date for each sku_id
        stock_info_df['base_available_to_sell'] = stock_info_df.apply(lambda row: get_base_available_to_sell(row,df=stock_info_df),axis=1)

        stock_info_df = stock_info_df[(stock_info_df['date'] == latest_date)].copy()
        stock_info_df.drop('date',axis=1,inplace=True)
        
        stock_info_df['url_markdown'] = stock_info_df['url'].map(lambda a : "[![Image Not Available](" + str(a) + ")](https://aemery.com)")  #get correctly formatted markdown to display images in data_table
        
        stock_info_df = stock_info_df.apply(get_extra_data, args = (po_df,orders_df),axis=1) #get extra data based on order and po info

        stock_info_df = stock_info_df[['url_markdown','e_date','date','season','p_name','color','size','base_available_to_sell','available_to_sell','additional_purchases','base_stock','online_sales_last_week', \
                             'wholesale_sales_last_week','online_sales_since_start','wholesale_sales_since_start','online_revenue_since_start','wholesale_revenue_since_start']]

        col_title_mapping = {'url_markdown':'Image','e_date':'Earliest Data','date':'Date','season':'Season(s)','p_name':'Product','color':'Colour','size':'Size','sku_id':'SKU', \
                             'in_stock':'In Stock','base_available_to_sell':'Base Available To Sell','available_to_sell':'Available To Sell','available_to_sell_from_stock':'Available To Sell From Stock', \
                             'additional_purchases': 'Additional Purchases','base_stock' : 'Base Stock','online_sales_last_week': 'Online Units Last Week','wholesale_sales_last_week' : 'Wholesale Units Last Week', \
                             'online_sales_since_start' : 'Online Units Since Start','wholesale_sales_since_start':'Wholesale Units Since Start','online_revenue_since_start':'Online $$$ Since Start', \
                             'wholesale_revenue_since_start':'Wholesale $$$ Since Start'}
    
        #common.logger.info(str(type(latest_date)) + str(latest_date) + str(type(date(1995,8,5))) + str(type(earliest_date.date())))
        

        diplay_columns = stock_info_df.columns.tolist()

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
                                    min_date_allowed = earliest_date.date(),
                                    max_date_allowed = latest_date.date(),
                                    initial_visible_month = earliest_date.date(),
                                    date = earliest_date.date(),
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
                            data=stock_info_df[display_columns][stock_info_df['date']==latest_date].to_dict("records"),
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
    global stock_info_df,display_columns,latest_date,earliest_date


    try:
        dff = stock_info_df.copy()
        group_list = []
        sum_list = ['available_to_sell','additional_purchases','base_stock','online_sales_last_week','wholesale_sales_last_week','online_sales_since_start',\
                    'wholesale_sales_since_start','online_revenue_since_start','wholesale_revenue_since_start']
        present_list = display_columns
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
        
        #df = stock_info_df[(stock_info_df['season'].str.contains('|'.join(v_seasons)))]
        #dff = stock_info_df[(stock_info_df['season'].str.contains('|'.join(v_seasons)))|(stock_info_df['p_name'].isin(v_product))|(stock_info_df['color'].isin(v_color))|(stock_info_df['size'].isin(v_size))]
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
        return df_grouped[present_list][df_grouped['date']==latest_date].to_dict("records")
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing layout')
        ) 

dash_app.layout = serve_layout
       