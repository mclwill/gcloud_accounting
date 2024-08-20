import os
import pandas as pd
from datetime import datetime
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
            df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            df = pd.DataFrame() #start with empty dataframe
        aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)
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

        byte_stream = common.read_dropbox_bytestream(customer,orders_file_path)
        if byte_stream:
            df = pd.read_csv(byte_stream,sep='|',index_col=False)
        else:
            df = pd.DataFrame() #start with empty dataframe

        queuedFiles =  common.getLocalFiles(os.path.join('home/gary/data_store',customer))
        if queuedFiles[0]:
            for file_item in queuedFiles[1]:
                data_lines = file_item['file_data'].split('\n')
                stream_id = cd_polling.get_CD_parameter(data_lines,'HD',3)
                if stream_id == 'OR':
                    action_id = cd_polling.get_CD_parameter(data_lines,'OR1',2)
                    if action_id == 'A':
                        channel = cd_polling.get_CD_parameter(data_lines,'OR1',12)
                        order_id = cd_polling.get_CD_parameter(data_lines,'OR1',3)
                        eans = cd_polling.get_CD_parameter(data_lines,'OR2',4)
                        if type(eans) == str:
                            eans = [eans]
                        qty_ordered = cd_polling.get_CD_parameter(data_lines,'OR2',7)
                        if type(qty_ordered) == str:
                            qty_ordered = [qty_ordered]
                        
                        for i in range(len(eans)):
                            row_dict = {}
                            row_dict['date_ordered'] = [file_item['mod_time'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
                            row_dict['order_id'] = [order_id]
                            row_dict['channel'] = [channel]
                            row_dict['ean'] = [eans[i]]
                            row_dict['qty_ordered'] = [qty_ordered[i]]
                            row_dict['OR'] = [True]

                            if not df.empty:
                                df = df.merge(pd.DataFrame.from_dict(row_dict),on=['order_id','ean'],how = 'outer',suffixes = ('','_y'))
                                df.drop(['date_ordered_y','channel_y','qty_ordered_y','OR_y'],axis=1,inplace=True)
                            else:
                                df = pd.DataFrame.from_dict(row_dict)
                            df.drop_duplicates(['order_id','channel','ean'],inplace=True)
                            common.logger.info('OR merge' + str(df.columns) + str(df.head()))
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
                        row_dict['date_shipped'] = [file_item['mod_time'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
                        row_dict['ean'] = [eans[i]]
                        row_dict['qty_shipped'] = [qty_shipped[i]]
                        row_dict['qty_variance'] = [qty_variance[i]]
                        row_dict['PC'] = [True]
                        # empty data
                        row_dict['date_shipped'] = [None]
                        row_dict['channel'] = [None]

                        if not df.empty:
                            df = df.merge(pd.DataFrame.from_dict(row_dict),on=['order_id','ean'],how = 'outer',suffixes = ('','_y'))
                            df.drop(['date_shipped_y','qty_shipped_y','qty_variance_y','PC_y'],axis=1,inplace=True,errors='ignore')
                        else:
                            df = pd.concat([df,pd.DataFrame.from_dict(row_dict)])
                        df.drop_duplicates(['order_id','channel','ean'],inplace=True)
                        common.logger.info('PC merge' + str(df.columns) + str(df.head()))
                #os.remove(os.path.join('home/gary/data_store',customer,file_item['file_name']))
        if not df.empty:
            csv_file_data = df.to_csv(sep='|',index=False)
            common.store_dropbox_unicode(customer,csv_file_data,orders_file_path)
            common.logger.info('Uphance orders DataStore updated for ' + customer + '\nFile Path: ' + orders_file_path)
        else:
            common.logger.info('Uphance orders DataStore not updated as dataframe was emtpy')
    
    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error retrieving data from Uphance' + '/nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))

    
