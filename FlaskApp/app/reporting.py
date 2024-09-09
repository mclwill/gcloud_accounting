import pandas as pd
import FlaskApp.app.common as common
from datetime import datetime
'''
module for collecting daily information on stock levels, sales, returns and new stock orders
Columns

Date,p_id,p_identifier,p_url,p_name,season_id,sku_id,sku_number,color,size,ean,in_stock,available_to_sell,available_to_sell_from_stock
'''

def get_uphance_stock_levels(customer):
	file_path = "/Wholesale/APIs (Anna's Dad)/Uphance/DataStore/data.csv"
	byte_stream = common.read_dropbox_bytestream(customer,file_path)
	if byte_stream:
		df = pd.read_csv(byte_stream,sep='|',index_col=False)
	else:
		df = pd.DataFrame() #start with empty dataframe
	
	url_product = 'https://api.uphance.com/products'
	page = 1 #get ready for Uphance pagination
	while page :
	    response = common.uphance_api_call(customer,'get',url=url_product+'/?page='+str(page))
	    common.logger.debug('Uphance Product API Call Status Code: ' + str(response[0]))
	    if response[0]:
	    	common.logger.warning('Uphance Error on Product API call for :' + customer)
	    	break
	    else:
		    data = response[1]
		    for p in data['products']:
		        row_dict = {}
		        row_dict['Date'] = [datetime.now()]
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
		            for sku in v['skus']:    
		                row_dict['sku_id'] = [sku['id']]
		                row_dict['size'] = [sku['size']]
		                row_dict['sku_number'] = [sku['sku_number']]
		                row_dict['in_stock'] = [sku['in_stock']]
		                row_dict['available_to_sell'] = [sku['available_to_sell']]
		                row_dict['available_to_sell_from_stock'] = [sku['available_to_sell_from_stock']]
		        df = pd.concat([df,pd.DataFrame.from_dict(row_dict)])
	    page = data['meta']['next_page']
	csv_file_data = df.to_csv(sep='|',index=False)
	common.store_dropbox_unicode(customer,csv_file_data,file_path)
	common.logger.info('Uphance DataStore updated for ' + customer + '\nFile Path: ' + file_path)