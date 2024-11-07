import os
import pandas as pd
from datetime import datetime, date, time, timedelta
from dateutil import tz
import traceback

import FlaskApp.app.common as common
import FlaskApp.app.cross_docks_polling as cd_polling
#from FlaskApp.app.pages.dashboard import get_data_from_data_store

'''
module for collecting daily information on stock levels, sales, returns and new stock orders
Columns

Date,p_id,p_identifier,p_url,p_name,season_id,sku_id,sku_number,color,size,ean,in_stock,available_to_sell,available_to_sell_from_stock
'''

utc_zone = tz.tzutc()
to_zone = tz.gettz('Australia/Melbourne')

customer = 'aemery'

data_store_folder = common.data_store[customer]
stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
orders_file_path = os.path.join(data_store_folder,'data_orders.csv')
po_file_path = os.path.join(data_store_folder,'data_po.csv')

stock_info_df = pd.DataFrame()

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

def get_data_store_info(customer):
    global season_df

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
        
        if in_between(aest_now.time(),time(22,30),time(6)) : #only do update between these times which is likely cronjob triggered rather than manual testing

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
                response = common.uphance_api_call(customer,'get',url=url_product+'/?page='+str(page),override=True)
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
                
                    df = pd.concat([df,pd.DataFrame.from_dict(row_dict)])
                    page = data['meta']['next_page']
            if season_df is not None:
                df['season'] = df.apply(lambda row: decode_season_id(row['season_id']),axis=1)
            csv_file_data = df.to_csv(sep='|',index=False)
            common.store_dropbox(customer,csv_file_data,stock_file_path,override=True)
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

        queuedFiles = common.get_dropbox_file_info(customer,os.path.join(orders_retrieve_path,'sent'),from_date=datetime.now()-timedelta(days=3),file_spec=['OR']) #use utc time as that is how dropbox stores file dates
        queuedFiles = queuedFiles + common.get_dropbox_file_info(customer,os.path.join(orders_retrieve_path,'received'),from_date=datetime.now()-timedelta(days=3),file_spec=['PC','TP'])
        #common.logger.info('debug data_orders 2')
        if queuedFiles:
            
            or_df = pd.DataFrame(columns = ['order_id','order_num','ean','date_ordered','channel','qty_ordered','OR'])
            pc_df = pd.DataFrame(columns = ['order_id','ean','date_shipped','qty_shipped','qty_variance','PC'])

            for file_item in queuedFiles:
                byte_stream = common.read_dropbox_bytestream('aemery',file_item['path_display'])
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
            common.store_dropbox(customer,orders_csv_file_data,orders_file_path,override=True)
            common.logger.info('Uphance orders DataStore updated for ' + customer + '\nFile Path: ' + orders_file_path)
        else:
            common.logger.info('Uphance orders DataStore not updated as dataframe was emtpy')

        if not po_df.empty:
            po_csv_file_data = po_df.to_csv(sep='|',index=False)
            common.store_dropbox(customer,po_csv_file_data,po_file_path,override=True)
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

get_data_from_data_store()  #only update datastore from here -> not via imports in other modules