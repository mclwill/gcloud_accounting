import os
import pandas as pd
from datetime import datetime, date, time, timedelta
from dateutil import tz
import traceback

import FlaskApp.app.common as common
import FlaskApp.app.cross_docks_polling as cd_polling
'''
module for collecting daily information on stock levels, sales, returns and new stock orders
Columns

Date,p_id,p_identifier,p_url,p_name,season_id,sku_id,sku_number,color,size,ean,in_stock,available_to_sell,available_to_sell_from_stock
'''

utc_zone = tz.tzutc()
to_zone = tz.gettz('Australia/Melbourne')

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
    s_list = s_id.split(',')
    for s in s_list:
        season = season_df.loc[s,'name'] 
        seasons.append(season)
    return ','.join(seasons)

def get_data_store_info(customer):
    global season_df

    try:
        data_store_folder = common.data_store[customer]
        stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
        orders_file_path = os.path.join(data_store_folder,'data_orders.csv')
        po_file_path = os.path.join(data_store_folder,'data_po.csv')
        orders_retrieve_path = common.access_secret_version('customer_parameters',customer,'dbx_folder')
        
        aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)
        
        if in_between(aest_now.time(),time(23),time(6)) : #only do update between these times which is likely cronjob triggered rather than manual testing

            #get season data from uphance
            url_seasons = 'https://api.uphance.com/seasons'
            response = common.uphance_api_call(customer,'get',url=url_seasons)
            
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
                df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'in_stock':'Int64','available_to_sell':'Int64','available_to_sell_from_stock':'Int64'})
            else:
                df = pd.DataFrame() #start with empty dataframe

            url_product = 'https://api.uphance.com/products'
            page = 1 #get ready for Uphance pagination
            while page :
                common.logger.debug('Request product dump from Uphance - Page : ' + str(page))
                response = common.uphance_api_call(customer,'get',url=url_product+'/?page='+str(page))
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
            common.store_dropbox_unicode(customer,csv_file_data,stock_file_path)
            common.logger.info('Uphance stock DataStore updated for ' + customer + '\nFile Path: ' + stock_file_path)

        #get order info from locally stored files
        stock_columns = ['order_id','ean','date_ordered','channel','qty_ordered','OR','date_shipped','qty_shipped','qty_variance','PC']
        po_columns = ['po_number','date_received','ean','qty_received']
        byte_stream = common.read_dropbox_bytestream(customer,orders_file_path)
        if byte_stream:
            orders_df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'qty_ordered':'Int64','qty_shipped':'Int64','qty_variance':'Int64','OR':"boolean",'PC':"boolean"})
        else:
            orders_df = pd.DataFrame(columns = stock_columns) #start with empty dataframe

        byte_stream = common.read_dropbox_bytestream(customer,po_file_path)
        if byte_stream:
            po_df = pd.read_csv(byte_stream,sep='|',index_col=False,dtype={'qty_received':'Int64'})
        else:
            po_df = pd.DataFrame(columns=po_columns) #start with empty dataframe

        queuedFiles = common.get_dropbox_file_info(customer,os.path.join(orders_retrieve_path,'sent'),from_date=datetime.now()-timedelta(days=10)) #use utc time as that is how dropbox stores file dates
        queuedFiles = queuedFiles + common.get_dropbox_file_info(customer,os.path.join(orders_retrieve_path,'received'),from_date=datetime.now()-timedelta(days=10))
        if queuedFiles:
            
            or_df = pd.DataFrame(columns = ['order_id','ean','date_ordered','channel','qty_ordered','OR'])
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
                        eans = cd_polling.get_CD_parameter(data_lines,'OR2',4)
                        if type(eans) == str:
                            eans = [eans]
                        qty_ordered = cd_polling.get_CD_parameter(data_lines,'OR2',5)
                        if type(qty_ordered) == str:
                            qty_ordered = [qty_ordered]
                        
                        for i in range(len(eans)):
                            row_dict = {}
                            row_dict['date_ordered'] = [file_item['client_modified'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
                            row_dict['order_id'] = [order_id]
                            row_dict['channel'] = [channel]
                            row_dict['ean'] = [eans[i]]
                            row_dict['qty_ordered'] = [qty_ordered[i]]
                            row_dict['OR'] = [True]

                            or_df = pd.concat([or_df,pd.DataFrame.from_dict(row_dict)])
                            or_df.drop_duplicates(subset=['order_id','channel','ean','date_ordered','date_shipped'],inplace=True,ignore_index=True) 
                            
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
                        pc_df.drop_duplicates(subset = ['order_id','channel','ean','date_ordered','date_shipped'],inplace=True,ignore_index=True) 

                elif stream_id == 'TP':
                    po_id = cd_polling.get_CD_parameter(data_lines,'TP',2)
                    if type(po_id) == str:
                        po_id = [po_id]
                    eans = cd_polling.get_CD_parameter(data_lines,'TP',4)
                    if type(eans) == str:
                        eans = [eans]
                    qty_received = cd_polling.get_CD_parameter(data_lines,'TP',5)
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


            merged_df = or_df.merge(pc_df,on=['order_id','ean'],how = 'outer',suffixes)

            if len(merged_df.index) > 0:
                orders_df = pd.concat([orders_df,merged_df])
                orders_df.drop_duplicates(subset = ['order_id','channel','ean','date_ordered','date_shipped'],inplace=True,ignore_index=True) 

        if not orders_df.empty:
            orders_csv_file_data = orders_df.to_csv(sep='|',index=False)
            common.store_dropbox_unicode(customer,orders_csv_file_data,orders_file_path)
            common.logger.info('Uphance orders DataStore updated for ' + customer + '\nFile Path: ' + orders_file_path)
        else:
            common.logger.info('Uphance orders DataStore not updated as dataframe was emtpy')

        if not po_df.empty:
            po_csv_file_data = po_df.to_csv(sep='|',index=False)
            common.store_dropbox_unicode(customer,po_csv_file_data,po_file_path)
            common.logger.info('Uphance purchase orders DataStore updated for ' + customer + '\nFile Path: ' + po_file_path)
        else:
            common.logger.info('Uphance purchase orders DataStore not updated as dataframe was emtpy')
    
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error retrieving data from Uphance' + '/nException Info: ' + str(ex) + '\nTraceback Info: \n' + str(tb))

    
