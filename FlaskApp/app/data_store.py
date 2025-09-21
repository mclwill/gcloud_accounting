import os
import pandas as pd
from datetime import datetime, date, time, timedelta
from dateutil import tz
import traceback
from flask_caching import Cache
from memory_profiler import profile
import sys
from io import StringIO
import numpy as np
from dash import html, dcc, callback, dash_table, clientside_callback
import re

import FlaskApp.app.common as common
import FlaskApp.app.cross_docks_polling as cd_polling
import FlaskApp.app.uphance_webhook_info as uphance_webhook
from FlaskApp.app import app


#from FlaskApp.app.pages.dashboard import get_data_from_data_store

'''
module for collecting daily information on stock levels, sales, returns and new stock orders
Columns

Date,p_id,p_identifier,p_url,p_name,season_id,sku_id,sku_number,color,size,ean,in_stock,available_to_sell,available_to_sell_from_stock
'''

utc_zone = tz.tzutc()
to_zone = tz.gettz('Australia/Melbourne')

customer = 'aemery'

mem_analysis = StringIO()

data_store_folder = common.data_store[customer]
stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
orders_file_path = os.path.join(data_store_folder,'data_orders.csv')
po_file_path = os.path.join(data_store_folder,'data_po.csv')

stock_info_df = pd.DataFrame()

CACHE_CONFIG = {
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 86400  #extend cache across time period of one day.
}
cache = Cache()
cache.init_app(app, config=CACHE_CONFIG)

def in_between(now, start, end):
    if start <= end:
        return start <= now < end
    else: # over midnight e.g., 23:30-04:15
        return start <= now or now < end

def dict_append(d,k,v): #used to build row_dict array in format to transfer to pandas dataframe
    if k in d:
        d[k].append(v)
        return d[k]
    else:
        return [v]

def decode_season_id(s_id):
    global season_df

    seasons = []
    s_list = str(s_id).split(',')  #to avoid errors where int makes its way to here - see error 12 Sep 2024 at 11.31pm
    for s in s_list:
        season = season_df.loc[s,'name'] 
        seasons.append(season)
    return ', '.join(seasons)

@profile(stream=mem_analysis)
def pd_concat_with_mem(dfs, time,text,iteration):
    old_stdout = sys.stdout 
    sys.stdout = mem_analysis
    print('Memory data at ' + str(time) + ' for ' + text + ' iteration no. ' + str(iteration) + ' ' + str(time))
    return_df = pd.concat(dfs)
    print('End of Data')
    sys.stdout = old_stdout
    return return_df


def get_data_store_info(customer):
    global season_df, mem_analysis

    try:
        common.logger.debug('getting data store info')
        data_store_folder = common.data_store[customer]
        stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
        orders_file_path = os.path.join(data_store_folder,'data_orders.csv')
        po_file_path = os.path.join(data_store_folder,'data_po.csv')
        orders_retrieve_path = common.access_secret_version('customer_parameters',customer,'dbx_folder')
        
        if common.running_local:
            aest_now = datetime.now()
        else:
            aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)
        
        if in_between(aest_now.time(),time(20,30),time(6)) : #only do update between these times which is likely cronjob triggered rather than manual testing

            #get season data from uphance
            url_seasons = 'https://api.uphance.com/seasons'
            response = common.uphance_api_call(customer,'get',url=url_seasons,override=True)
            
            if response[0]:
                common.logger.warning('Uphance Error on Season API call for :' + customer)
                season_df = None
            else:
                data = response[1]
                season_df = pd.DataFrame.from_dict(data['seasons'])
                season_df['id'] = season_df['id'].astype(str)
                season_df.set_index('id',inplace=True)

            #get stock level info from Uphance

            
            byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
            if byte_stream:
                df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'in_stock':'Int64','available_to_sell':'Int64','available_to_sell_from_stock':'Int64','season_id':str,'size':str,'ean':str})
            else:
                df = pd.DataFrame() #start with empty dataframe

            url_product = 'https://api.uphance.com/products'
            page = 1 #get ready for Uphance pagination
            while page :
                common.logger.debug('Request product dump from Uphance - Page : ' + str(page))
                response = common.uphance_api_call(customer,'get',url=url_product+'/?page='+str(page),override=False)
                common.logger.debug('Uphance Product API Call Status Code: ' + str(response[0]))
                if response[0]:
                    common.logger.warning('Uphance Error on Product API call for :' + customer)
                    break
                else:
                    data = response[1]
                    row_dict = {}
                    for p in data['products']:
                        for v in p['variations'] :
                            for sku in v['skus']:  
                                if sku['ean']:
                                    row_dict['date'] = dict_append(row_dict,'date',aest_now)
                                    row_dict['p_id'] = dict_append(row_dict,'p_id',p['id'])
                                    row_dict['p_identifier'] = dict_append(row_dict,'p_identifier',p['product_identifier'])
                                    row_dict['p_name'] = dict_append(row_dict,'p_name',p['name'])
                                    row_dict['url'] = dict_append(row_dict,'url',p['image_url'])
                                    row_dict['season_id'] = dict_append(row_dict,'season_id',','.join(str(x) for x in p['season_id']))
                                    row_dict['category'] = dict_append(row_dict,'category',p['category'])
                                    row_dict['sub_category'] = dict_append(row_dict,'sub_category',p['sub_category'])
                                    row_dict['color'] = dict_append(row_dict,'color',v['color'])
                                    row_dict['sku_id'] = dict_append(row_dict,'sku_id',sku['id'])
                                    row_dict['ean'] = dict_append(row_dict,'ean',sku['ean'])
                                    row_dict['size'] = dict_append(row_dict,'size',sku['size'])
                                    row_dict['sku_number'] = dict_append(row_dict,'sku_number',sku['sku_number'])
                                    row_dict['in_stock'] = dict_append(row_dict,'in_stock',sku['in_stock'])
                                    row_dict['available_to_sell'] = dict_append(row_dict,'available_to_sell',sku['available_to_sell'])
                                    row_dict['available_to_sell_from_stock'] = dict_append(row_dict,'available_to_sell_from_stock',sku['available_to_sell_from_stock'])
                                    for pr in v['prices']:
                                        row_dict['price_' + pr['name'] + '_currency' ] = dict_append(row_dict,'price_' + pr['name'] + '_currency',pr['currency'])
                                        row_dict['price_' + pr['name'] + '_wsp' ] = dict_append(row_dict,'price_' + pr['name'] + '_wsp',pr['wsp_money'])
                                        row_dict['price_' + pr['name'] + '_mrsp' ] = dict_append(row_dict,'price_' + pr['name'] + '_mrsp' ,pr['msrp_money'])
                
                    df = pd_concat_with_mem([df,pd.DataFrame.from_dict(row_dict)],aest_now,'Stock Data concat',page)
                    #df = pd.concat([df,pd.DataFrame.from_dict(row_dict)])
                    page = data['meta']['next_page']
            
            common.send_email(0,'Memory Analysis in Data Store',mem_analysis.getvalue(),'gary@mclarenwilliams.com.au')

            if season_df is not None:
                df['season'] = df.apply(lambda row: decode_season_id(row['season_id']),axis=1)
            
            #filter out records to be just Sunday's of every week for dates before 90 days
            df['datetime'] = pd.to_datetime(df['date'])
            df['day_of_week'] = df['datetime'].dt.dayofweek
            df['timedelta'] = (aest_now - df['datetime']).dt.days
            df_filtered = df[((df['day_of_week']==6)&(df['timedelta']>90))|(df['timedelta']<=90)] #only include Sunday if older than 90 days
            
            #df_filtered.to_csv('test_stock.csv',sep='|',index=False)
            df_filtered.drop(columns=['datetime','day_of_week','timedelta'],inplace=True)
            csv_file_data = df_filtered.to_csv(sep='|',index=False)
            common.store_dropbox(customer,csv_file_data,stock_file_path,override=False)
            common.logger.info('Uphance stock DataStore updated for ' + customer + '\nFile Path: ' + stock_file_path)

        #common.logger.info('debug data_orders')
        #get order info from locally stored files
        stock_columns = ['order_id','order_num','ean','date_ordered','channel','qty_ordered','OR','date_shipped','qty_shipped','qty_variance','PC']
        po_columns = ['po_number','date_received','ean','qty_received']
        byte_stream = common.read_dropbox_bytestream(customer,orders_file_path)
        if byte_stream:
            orders_df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'qty_ordered':'Int64','qty_shipped':'Int64','qty_variance':'Int64','OR':"boolean",'PC':"boolean",'ean':str,'order_num':str})
        else:
            orders_df = pd.DataFrame(columns = stock_columns) #start with empty dataframe

        byte_stream = common.read_dropbox_bytestream(customer,po_file_path)
        if byte_stream:
            po_df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'qty_received':'Int64','ean':str})
        else:
            po_df = pd.DataFrame(columns=po_columns) #start with empty dataframe

        queuedFiles = common.get_dropbox_file_info(customer,os.path.join(orders_retrieve_path,'sent'),from_date=datetime.now()-timedelta(days=7),file_spec=['OR']) #use utc time as that is how dropbox stores file dates
        queuedFiles = queuedFiles + common.get_dropbox_file_info(customer,os.path.join(orders_retrieve_path,'received'),from_date=datetime.now()-timedelta(days=7),file_spec=['PC','TP'])
        #common.logger.info('debug data_orders 2')
        if queuedFiles:
            
            or_df = pd.DataFrame(columns = ['order_id','order_num','ean','date_ordered','channel','qty_ordered','OR'])
            pc_df = pd.DataFrame(columns = ['order_id','ean','date_shipped','qty_shipped','qty_variance','PC'])

            for file_item in queuedFiles:
                byte_stream = common.read_dropbox_bytestream('aemery',file_item['path_display'])
                if byte_stream:
                    data_lines = byte_stream.read().decode('utf=8').split('\n')
                    stream_id = cd_polling.get_CD_parameter(data_lines,'HD',3)
                    if stream_id == 'OR':
                        action_id = cd_polling.get_CD_parameter(data_lines,'OR1',2)
                        if action_id == 'A':
                            channel = cd_polling.get_CD_parameter(data_lines,'OR1',14)
                            order_id = cd_polling.get_CD_parameter(data_lines,'OR1',3)
                            order_num = cd_polling.get_CD_parameter(data_lines,'OR1',6)
                            eans = cd_polling.get_CD_parameter(data_lines,'OR2',4)
                            if type(eans) == str:
                                eans = [eans]
                            qty_ordered = cd_polling.get_CD_parameter(data_lines,'OR2',5)
                            if type(qty_ordered) == str:
                                qty_ordered = [qty_ordered]
                            
                            if eans: #some OR files processed without items for some reason
                                for i in range(len(eans)):
                                    row_dict = {}
                                    row_dict['date_ordered'] = [file_item['client_modified'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
                                    row_dict['order_id'] = [order_id]
                                    row_dict['order_num'] = [order_num]
                                    row_dict['channel'] = [channel]
                                    row_dict['ean'] = [eans[i]]
                                    row_dict['qty_ordered'] = [qty_ordered[i]]
                                    row_dict['OR'] = [True]

                                    or_df = pd.concat([or_df,pd.DataFrame.from_dict(row_dict)])
                                    or_df.drop_duplicates(subset=['order_id','channel','ean','date_ordered'],inplace=True,ignore_index=True) 
                                
                    elif stream_id == 'PC':
                        order_id = cd_polling.get_CD_parameter(data_lines,'OS1',2)
                        eans = cd_polling.get_CD_parameter(data_lines,'OS2',2)
                        if type(eans) == str:
                            eans = [eans]
                        qty_shipped = cd_polling.get_CD_parameter(data_lines,'OS2',4)
                        if type(qty_shipped) == str:
                            qty_shipped = [qty_shipped]
                        qty_variance =cd_polling.get_CD_parameter(data_lines,'OS2',5)
                        if type(qty_variance) == str:
                            qty_variance = [qty_variance]
                        
                        for i in range(len(eans)):
                            row_dict = {}
                            row_dict['order_id'] = [order_id]
                            row_dict['date_shipped'] = [file_item['client_modified'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
                            row_dict['ean'] = [eans[i]]
                            row_dict['qty_shipped'] = [qty_shipped[i]]
                            row_dict['qty_variance'] = [qty_variance[i]]
                            row_dict['PC'] = [True]

                            pc_df = pd.concat([pc_df,pd.DataFrame.from_dict(row_dict)])
                            pc_df.drop_duplicates(subset = ['order_id','ean','date_shipped'],inplace=True,ignore_index=True) 

                    elif stream_id == 'TP':
                        po_id = cd_polling.get_CD_parameter(data_lines,'TP',2)
                        if type(po_id) == str:
                            po_id = [po_id]
                        eans = cd_polling.get_CD_parameter(data_lines,'TP',5)
                        if type(eans) == str:
                            eans = [eans]
                        qty_received = cd_polling.get_CD_parameter(data_lines,'TP',6)
                        if type(qty_received) == str:
                            qty_received = [qty_received]
                        
                        for i in range(len(eans)):
                            row_dict = {}
                            row_dict['po_number'] = [po_id[i]]
                            row_dict['date_received'] = [file_item['client_modified'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
                            row_dict['ean'] = [eans[i]]
                            row_dict['qty_received'] = [qty_received[i]]

                            po_df = pd.concat([po_df,pd.DataFrame.from_dict(row_dict)])
                            po_df.drop_duplicates(subset=['po_number','ean','date_received'],inplace=True,ignore_index=True) 
                else:
                    common.logger.warning('Unable to get file from dropbox: ' + str(file_item) + '\nProcessing of queued files will be abored')
                    break

            merged_df = or_df.merge(pc_df,on=['order_id','ean'],how = 'outer')

            if len(merged_df.index) > 0:
                orders_df = pd.concat([orders_df,merged_df])
                orders_df['order_num'] = orders_df['order_num'].str.replace('.0','',regex=False) #noticed some integer values had been converted to strings
                orders_df.drop_duplicates(subset = ['order_id','channel','ean','date_ordered','date_shipped'],inplace=True,ignore_index=True)
                dedup_col_list = orders_df.columns.tolist() #list of columns to drop after merge
                orders_df.drop([c for c in dedup_col_list if (('_x' in c) or ('_y' in c))],inplace=True,errors='ignore')
                orders_df = orders_df.groupby(['order_id','ean'],as_index=False).first() #make sure we have grouped all orders - may miss some if OR and PC across different downloads

        #common.logger.info('debug data_orders 3')
        if not orders_df.empty:
            orders_csv_file_data = orders_df.to_csv(sep='|',index=False)
            common.store_dropbox(customer,orders_csv_file_data,orders_file_path,override=False)
            common.logger.info('Uphance orders DataStore updated for ' + customer + '\nFile Path: ' + orders_file_path)
        else:
            common.logger.info('Uphance orders DataStore not updated as dataframe was emtpy')

        if not po_df.empty:
            po_csv_file_data = po_df.to_csv(sep='|',index=False)
            common.store_dropbox(customer,po_csv_file_data,po_file_path,override=False)
            common.logger.info('Uphance purchase orders DataStore updated for ' + customer + '\nFile Path: ' + po_file_path)
        else:
            common.logger.info('Uphance purchase orders DataStore not updated as dataframe was emtpy')

        
        common.logger.debug('finished getting data store info')
        common.logger.debug('finished updating data from data store')
        get_data_from_data_store()
    
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error retrieving data from Uphance' + '/nException Info: ' + str(ex) + '\nTraceback Info: \n' + str(tb))

    
def get_data_from_data_store():

    global stock_info_df,orders_df,po_df
    global latest_date,earliest_date,aest_now,default_end_season_date
    global start_of_previous_week,end_of_previous_week
    

    try:
        #collect data in serve_layout so that latest is retrieved from data_store

        #tb = traceback.format_stack()
        #common.logger.info('Traceback for get_data :' + '\n' + str(tb))

        #flush_cache() #ensure cache is flush before getting data from data store to make sure it doesn't get too big.

        if common.running_local:
            aest_now = datetime.now()
        else:
            aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)

        #get stock info from data store
        byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
        if byte_stream:
            stock_info_df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'in_stock':'Int64','available_to_sell':'Int64','available_to_sell_from_stock':'Int64','season_id':str,'size':str,'ean':str})
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
            orders_df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'qty_ordered':'Int64','qty_shipped':'Int64','qty_variance':'Int64','OR':"boolean",'PC':"boolean",'ean':str})
        else:
            orders_df = pd.DataFrame() #start with empty dataframe

        #if orders_df.empty:
        #    return #html.Div(html.P('No Orders Data retrieved from Data Store'))

        #get po info from data store
        byte_stream = common.read_dropbox_bytestream(customer,po_file_path)
        if byte_stream:
            po_df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'qty_received':'Int64','ean':str})
        else:
            po_df = pd.DataFrame() #start with empty dataframe

        if po_df.empty or orders_df.empty:
            return #html.Div(html.P('No Purchase Orders Data retrieved from Data Store'))

        #convert date column to_datetime in all dfs - ie drop time info
        po_df['date_received'] = pd.to_datetime(po_df['date_received']).dt.date
        orders_df['date_ordered'] = pd.to_datetime(orders_df['date_ordered']).dt.date
        orders_df['date_shipped'] = pd.to_datetime(orders_df['date_shipped']).dt.date
        stock_info_df['date'] = stock_info_df['date'].dt.date
        stock_info_df['size'] = stock_info_df['size'].astype(str) #make sure these are all strings for sorting purposes

        #stock_info_df['e_date'] = stock_info_df.apply(lambda row: get_earliest_date(row,df=stock_info_df),axis=1) #get earliest inventory date for each sku_id - uses simply apply to find minimum on a SKU basis

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))     

def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)

def get_start_of_previous_week(date_value):
    #weekday = date_value.weekday()
    #monday_delta = timedelta(days=weekday,weeks=1)
    seven_days = timedelta(days=7)
    return date_value - seven_days

def get_earliest_date(row,df):
    #this should be the earliest non-zero inventory date
    #need to find way to speed this process up as it is taking over 20 min - 11 Oct 2024
    #maybe need calculate once for each 'sku_id' and then copy from the last record
    return df['date'][(df['sku_id'] == row['sku_id'])&(df['available_to_sell']>0)].min()

def get_data_from_globals():
    global stock_info_df,orders_df,po_df
    global latest_date,earliest_date,default_end_season_date
    global start_of_previous_week,end_of_previous_week

    return stock_info_df,orders_df,po_df,\
           latest_date,earliest_date,default_end_season_date,\
           start_of_previous_week,end_of_previous_week

def get_base_available_to_sell(df,base_start_date):
    #global base_start_date
    #common.logger.info(str(df[(df['sku_id'] == row['sku_id'])&(df['date']==base_start_date)].loc[:,'available_to_sell_from_stock'].values))
    return_df = df[['ean','available_to_sell_from_stock']][(df['date']==base_start_date)]
    return_df.rename({'available_to_sell_from_stock':'base_available_to_sell'},inplace=True,axis=1)
    return_df.set_index('ean',inplace=True)
    return return_df['base_available_to_sell']

def get_last_week_orders(df,base_start_date):
    global start_of_previous_week,end_of_previous_week#base_start_date

    df = df[(df['OR'])&(df['PC'])] #make sure only count orders where we have OR file and PC file
    if start_of_previous_week < base_start_date :
        start_date = base_start_date
    else:
        start_date = start_of_previous_week
    common.logger.debug(str(start_date) + '_______' + str(end_of_previous_week))

    return df.assign(result=np.where((df['date_ordered']>=start_date)&(df['date_ordered']<=end_of_previous_week),df['qty_ordered'],0)).groupby('ean').agg({'result':'sum'})

def get_orders_since_start(df,base_start_date):
    #global base_start_date
    df = df[(df['OR'])&(df['PC'])] #make sure only count orders where we have OR file and PC file
    return df.assign(result=np.where(df['date_ordered']>=base_start_date,df['qty_ordered'],0)).groupby('ean').agg({'result':'sum'})

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
        
        base_available_to_sell_df = get_base_available_to_sell(stock_info_df[['ean','date','available_to_sell_from_stock']],base_start_date).rename('base_available_to_sell') #get base_data for start of season calcs - returns DF with 'ean' as index and 'base_available_to_sell' column 

        base_stock_info_df.set_index('ean',inplace=True) #set stock DF with 'ean' as index in preparation for join
        base_stock_info_df = base_stock_info_df.join(base_available_to_sell_df) #do join on 'ean'
        base_stock_info_df.reset_index(inplace=True) #reset index 
        
        common.logger.debug('Base data merge complete - starting collection of po and orders DFs')

        base_stock_info_df = base_stock_info_df[(base_stock_info_df['date'] == latest_date)].copy()#get rid of all stock rows that are before latest date - don't need them anymore
        base_stock_info_df['url_markdown'] = base_stock_info_df['url'].map(lambda a : "[![Image Not Available](" + str(a) + ")](https://aemery.com)")  #get correctly formatted markdown to display images in data_table
        common.logger.debug(str(base_stock_info_df[['ean','category','sub_category','in_stock','available_to_sell_from_stock','base_available_to_sell']]))
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
        online_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']=='eCommerce - online'],base_start_date).rename(columns={'result':'online_orders_last_7_days'})#.rename('online_orders_prev_week')
        online_orders_prev_week_df.index = online_orders_prev_week_df.index.astype(str)
        wholesale_orders_prev_week_df = get_last_week_orders(orders_df[orders_df['channel']=='eCommerce - reorder'],base_start_date).rename(columns={'result':'wholesale_orders_last_7_days'})#.rename('wholesale_orders_prev_week')
        wholesale_orders_prev_week_df.index = wholesale_orders_prev_week_df.index.astype(str)
        #common.logger.debug(str(online_orders_prev_week_df))
        #online_orders_prev_week_df.to_csv('online.csv')
        #get online and wholesale since start orders with 'ean' as index of type string
        online_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']=='eCommerce - online']),base_start_date).rename(columns={'result':'online_orders_since_start'})#.rename('online_orders_since_start')
        online_orders_since_start_df.index = online_orders_since_start_df.index.astype(str)
        wholesale_orders_since_start_df = get_orders_since_start((orders_df[orders_df['channel']=='eCommerce - reorder']),base_start_date).rename(columns={'result':'wholesale_orders_since_start'})#.rename('wholesale_orders_since_start')  
        wholesale_orders_since_start_df.index = wholesale_orders_since_start_df.index.astype(str)

        common.logger.debug('Finished collection of po and order info - starting merge of PO and order info into Stock DF')
        common.logger.debug(str(base_stock_info_df.columns))
        common.logger.debug(str(base_stock_info_df[['category','sub_category','in_stock','available_to_sell_from_stock','base_available_to_sell']]))
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
        base_stock_info_df['online_orders_last_7_days'] = base_stock_info_df['online_orders_last_7_days'].fillna(0)
        base_stock_info_df['wholesale_orders_last_7_days'] = base_stock_info_df['wholesale_orders_last_7_days'].fillna(0)
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
        base_stock_info_df['estimated_sell_out_weeks'] = base_stock_info_df['available_to_sell_from_stock'] / base_stock_info_df['daily_sell_rate'] / 7
        
        #fix up any divide by zeroes
        base_stock_info_df[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']] = base_stock_info_df[['online_pc_since_start','wholesale_pc_since_start','seasonal_sell_through_pc','daily_sell_rate','estimated_sell_out_weeks']].replace([np.inf,-np.inf],np.nan)
        
        common.logger.debug('finished vectored operations - data manipulation and merge complete')
        common.logger.debug(str(base_stock_info_df))
        #base_stock_info_df.to_csv('/Users/Mac/Downloads/stock_info_end.csv')

        #base_stock_info_df[['online_orders_last_7_days','online_orders_since_start']][base_stock_info_df['online_orders_last_7_days']>0].to_csv('online.csv')
        return base_stock_info_df

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))

@cache.memoize()
def global_store(base_start_date):
    try:
        common.logger.debug('Base Start Date in global_store' + str(type(base_start_date)) + '\n' + str(base_start_date))
        if type(base_start_date) == str:
                base_start_date = datetime.strptime(base_start_date,'%Y-%m-%d').date()
        return process_data(base_start_date)
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Exception in Cache Global Store: ' + str(ex)  + '/nTraceback Info: ' + str(tb))

def flush_cache():
    with app.app_context():
        cache.clear()

def get_master_IT_file(customer):
    try:
        #get all products using pagination
        url_product = 'https://api.uphance.com/products'
        product_pages = []
        file_data = ''
        result_dict = {}
        result_dict['error'] = []

        page = 1 #get ready for Uphance pagination
        while page :
            common.logger.debug('Request product dump from Uphance for master IT dump - Page : ' + str(page))
            response = common.uphance_api_call(customer,'get',url=url_product+'/?page='+str(page),override=False)
            common.logger.debug('Uphance Product API Call Status Code: ' + str(response[0]))
            if response[0]:
                common.logger.warning('Uphance Error on Product API call for :' + customer)
                break
            else:
                data = response[1]
                product_pages.append(data)
                page = data['meta']['next_page']
        i = 0
        for page in product_pages:
            for p in page['products']:
                i += 1
                result = uphance_webhook.process_product_update(customer,p,master=True,override=False)
                for k,v in result[1].items():
                    if k == 'error':
                        common.logger.debug(str(k))
                        common.logger.debug(str(v))
                        for e in v:
                            common.logger.debug(str(e))
                            result_dict['error'].append(e)
                    else:
                        result_dict[k] = v
                if i == 1:
                    file_data = file_data + result[0]
                else:
                    file_data = file_data + re.sub("HD\|([A-Z]){2}\|IT\n",'',result[0])
                    #file_data + result[0].replace('HD|EM|IT\n','')
                    #file_data = file_data + result[0].replace('HD|TT|IT\n','')

        if common.running_local:
            aest_now = datetime.now()
        else:
            aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)

        file_name = 'IT_FULL_' + aest_now.strftime('%Y%m%d') + '.csv'
        common.logger.debug(file_name)
        result_dict = uphance_webhook.process_file(customer,file_data,file_name,result_dict)
        common.logger.debug('result_dict: ' + str(result_dict))
        
        error_send_to_CD = None
        for error in result_dict['error']:
            if 'send_to_CD' in error:
                    error_send_to_CD = error
                    break
        if error_send_to_CD:
            if error_send_to_CD['send_to_CD']:
                error_message = 'Master IT file sent to CD' 
            else:
               error_message = 'Master IT file NOT sent to CD' 
            common.send_email(0,'Master IT file Processing',error_message + '\n\nError Info: ' + str(result_dict['error']) + '\n' + 'Output file:\n' + file_name,['global'],customer=customer)
 

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Exception in retrieving and processing Master IT file: ' + str(ex)  + '/nTraceback Info: ' + str(tb))


get_data_from_data_store()  #only update datastore from here -> not via imports in other modules