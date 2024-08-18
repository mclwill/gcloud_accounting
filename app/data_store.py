import pandas as pd
import FlaskApp.app.common as common
from datetime import datetime
import FlaskApp.app.cross_docks_polling as cd_polling
'''
module for collecting daily information on stock levels, sales, returns and new stock orders
Columns

Date,p_id,p_identifier,p_url,p_name,season_id,sku_id,sku_number,color,size,ean,in_stock,available_to_sell,available_to_sell_from_stock
'''



utc_zone = tz.tzutc()
to_zone = tz.gettz('Australia/Melbourne')

def extend_list_in_dict(row_dict,n):
	for d in row_dict.keys():
		for i in range(n)L
			row_dict[d] = row_dict[d].append(row_dict[d][-1])

def get_uphance_data_store_info(customer):
	data_store_folder = common.access_secret_version('customer_parameters',customer,'data_store_folder')
	stock_file_path = os.path.join(data_store_folder,'data_stock.csv')
	orders_file_path = os.path.join(data_store_folder,'data_orders.csv')

	#get stock level info from Uphance

	byte_stream = common.read_dropbox_bytestream(customer,stock_file_path)
	if byte_stream:
		df = pd.read_csv(byte_stream,sep='|',index_col=False)
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
		    aest_now = datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)
		    row_df = pd.DataFrame()
		    for p in data['products']:
		        row_dict = {}
		        row_dict['date'] = [aest_now]
		        row_dict['p_id'] = [p['id']]
		        row_dict['p_identifier'] = [p['product_identifier']]
		        row_dict['p_name'] = [p['name']]
		        row_dict['url'] = [p['image_url']]
		        row_dict['season_id'] = [','.join(str(x) for x in p['season_id'])]
		        row_dict['category'] = [p['category']]
		        row_dict['sub_category'] = [p['sub_category']]

		        for v in p['variations'] :
		            row_dict['color'] = [v['color']]
		            for pr in v['prices']:
		                row_dict['price_' + pr['name'] + '_currency' ] = [pr['currency']]
		                row_dict['price_' + pr['name'] + '_wsp' ] = [pr['wsp_money']]
		                row_dict['price_' + pr['name'] + '_mrsp' ] = [pr['msrp_money']]
		                row_df = 
		            for sku in v['skus']:    
		                row_dict['sku_id'] = [sku['id']]
		                row_dict['size'] = [sku['size']]
		                row_dict['sku_number'] = [sku['sku_number']]
		                row_dict['ean'] = [sku['ean']]
		                row_dict['in_stock'] = [sku['in_stock']]
		                row_dict['available_to_sell'] = [sku['available_to_sell']]
		                row_dict['available_to_sell_from_stock'] = [sku['available_to_sell_from_stock']]
		        		df = pd.concat([df,pd.DataFrame.from_dict(row_dict)])
	    page = data['meta']['next_page']
	csv_file_data = df.to_csv(sep='|',index=False)
	common.store_dropbox_unicode(customer,csv_file_data,stock_file_path)
	common.logger.info('Uphance stock DataStore updated for ' + customer + '\nFile Path: ' + stock_file_path)

	#get order info from locally stored files

	byte_stream = common.read_dropbox_bytestream(customer,orders_file_path)
	if byte_stream:
		df = pd.read_csv(byte_stream,sep='|',index_col=False)
	else:
		df = pd.DataFrame() #start with empty dataframe

	queuedFiles =  common.getLocalFiles(os.path.join()'home/gary/data_store',customer) :
	if queuedFiles[0]:
		for file_item in queuedFiles[1]:
			data_lines = file_item['file_data'].split('\n')
			stream_id = get_CD_parameter(data_lines,'HD',3)
			if stream_id == 'OR':
				channel = cd_polling.get_CD_parameter(data_lines,'OR1',12)
				order_id = cd_polling.get_CD_parameter(data_lines,'OR1',3)
				eans = cd_polling.get_CD_parameter(data_lines,'OR2',4)
				qty_ordered = cd_polling.get_CD_parameter(data_lines,'OR2',7)
				
				for i in len(eans):
					row_dict = {}
					row_dict['date_ordered'] = [file_item['mod_time'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
					row_dict['order_id'] = [order_id]
					row_dict['channel'] = [channel]
					row_dict['ean'] = [eans[i]]
					row_dict['qty_ordered'] = [qty_ordered[i]]

					df = df.merge(pd.DataFrame.from_dict(row_dict),on=['order_id','ean'],how='outer')
			
			elif stream_id == 'PC':
				order_id = cd_polling.get_CD_parameter(data_lines,'OS1',2)
				eans = cd_polling.get_CD_parameter(data_lines,'OS2',2)
				qty_shipped = cd_polling.get_CD_parameter(data_lines,'OR2',4)
				qty_variance =cd_polling.get_CD_parameter(data_lines,'OR2',5)
				
				for i in len(eans):
					row_dict = {}
					row_dict['order_id'] = [order_id]
					row_dict['date_shipped'] = [file_item['mod_time'].replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
					row_dict['ean'] = [eans[i]]
					row_dict['qty_shipped'] = [qty_shipped[i]]
					row_dict['qty_variance'] = [qty_variance[i]]

					df = df.merge(pd.DataFrame.from_dict(row_dict),on=['order_id','ean'],how='outer')
	csv_file_data = df.to_csv(sep='|',index=False)
	common.store_dropbox_unicode(customer,csv_file_data,orders_file_path)
	common.logger.info('Uphance orders DataStore updated for ' + customer + '\nFile Path: ' + orders_file_path)

def pc_file_received(customer,data_lines):
	global orders_file_path

	byte_stream = common.read_dropbox_bytestream(customer,orders_file_path)
	if byte_stream:
		df = pd.read_csv(byte_stream,sep='|',index_col=False)
	else:
		df = pd.DataFrame() #start with empty dataframe

	data_lines = file_data.split('\n')
	order_id = cd_polling.get_CD_parameter(data_lines,'OS1',2)
	eans = cd_polling.get_CD_parameter(data_lines,'OS2',2)
	qty_shipped = cd_polling.get_CD_parameter(data_lines,'OR2',4)
	qty_variance =cd_polling.get_CD_parameter(data_lines,'OR2',5)
	
	for i in len(eans):
		row_dict = {}
		row_dict['order_id'] = [order_id]
		row_dict['date_shipped'] = [datetime.now().replace(tzinfo=utc_zone).astimezone(to_zone).replace(tzinfo=None)]
		row_dict['ean'] = [eans[i]]
		row_dict['qty_ordered'] = [qty_ordered[i]]

	df = pd.concat([df,pd.DataFrame.from_dict(row_dict)])
	csv_file_data = df.to_csv(sep='|',index=False)
	common.store_dropbox_unicode(customer,csv_file_data,orders_file_path)
	common.logger.info('Uphance orders DataStore updated for ' + customer + '\nFile Path: ' + orders_file_path)
