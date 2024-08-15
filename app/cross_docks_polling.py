import json
import io
import datetime
from dateutil import tz
import ftputil
import requests
import traceback
from tabulate import tabulate

import FlaskApp.app.common as common 

from_zone = tz.tzutc()
to_zone = tz.tzlocal()

def get_pending_FTP_files(customer):
    cross_docks_info = common.get_CD_FTP_credentials(customer)
    try: 
        with ftputil.FTPHost("ftp.crossdocks.com.au", cross_docks_info['username'], cross_docks_info['password']) as ftp_host:

            ftp_host.chdir('out/pending')
            pending_files = ftp_host.listdir(ftp_host.curdir)

            ftp_host.chdir('../../in/rejected')  #check for any rejected files
            rejected_files = ftp_host.listdir(ftp_host.curdir)

            if rejected_files:
                common.send_email(0,'CD_FTP_Rejected_Files','Rejected Files reported by Cross Docks:\n' + str(rejected_files),['global'],customer=customer)
    
    except Exception as ex:
        common.logger.warning('Cross Docks Error on getting pending files for ' + customer + '\nException : ' + str(ex))
        return False

    common.logger.debug('Files returned for ' + customer + ' from FTP curdir:' + str(pending_files))    
    return pending_files

def get_data_FTP(customer,directory,f):
    cross_docks_info = common.get_CD_FTP_credentials(customer)
    try: 
        with ftputil.FTPHost("ftp.crossdocks.com.au", cross_docks_info['username'], cross_docks_info['password']) as ftp_host:

            ftp_host.chdir(directory)
            with ftp_host.open(f,'r') as fobj:
                data = fobj.read()
    
    except Exception as ex:
        common.logger.warning('Error in get_data_FTP for ' + customer +': ' + str(ex))
        return False
        
    return data

def move_CD_file_FTP(customer,source,dest,f):
    cross_docks_info = common.get_CD_FTP_credentials(customer)
    try: 
        with ftputil.FTPHost("ftp.crossdocks.com.au", cross_docks_info['username'], cross_docks_info['password']) as ftp_host:
            with ftp_host.open(source + '/' + f,'rb') as source_obj:
                with ftp_host.open(dest + '/' + f,'wb') as dest_obj:
                    ftp_host.copyfileobj(source_obj,dest_obj)
                    ftp_host.remove(source + '/' + f)
                    
    
    except Exception as ex:
        common.logger.warning('Error in move_CD_file_FTP for ' + customer + ':' + str(ex))
        return False
        
    return True
    

    common.logger.debug('CD file move for ' + customer + ' : ' + file)


def download_file_DBX(customer,file_data,folder,file):
    
    dbx_file = common.access_secret_version('customer_parameters',customer,'dbx_folder') + '/' + folder + '/' + file
    try:
        with io.BytesIO(file_data.encode()) as stream:
            stream.seek(0)
            common.dbx.files_upload(stream.read(), dbx_file, mode=common.dropbox.files.WriteMode.overwrite)

        return True
    except Exception as ex:
        common.logger.warning('DBX error for ' + customer + ': ' + str(ex))
        tb = traceback.format_exc()
        common.logger.warning(tb)
        return False
    
    common.logger.debug('DBX download for ' + customer + ' : ' + file)
    
def get_CD_parameter(data,ri,col_id):
    #return all records with ri indicator as a list
    info = []
    for line in data:
        if line.split('|')[0] == ri:
            info.append(line.split('|')[col_id-1].strip())
    
    if len(info) == 0:
        return None
    elif len(info) == 1:
        return info[0]
    else:
        return info
    return info

def uphance_api_call(customer,api_type,**kwargs):
    url = kwargs.pop('url',None)
    json = kwargs.pop('json',None)
    
    #this coding used for testing only so that Uphance is not updated
    #common.logger.info('Dummy API uphance call for ' + customer + '\n' + api_type  + str(url) + str(json))
    #return 500, 'Testing Call to uphance_api_call'
    #end of testing code

    return_error = False
    
    if api_type == 'post':
        response = requests.post(url,json = json,headers = common.uphance_headers[customer])
        common.logger.debug('Post ' + url)
    elif api_type == 'put':
        response = requests.put(url,headers = common.uphance_headers[customer])
        common.logger.debug('Put ' + url)
    elif api_type == 'get' :
        response = requests.get(url,headers = common.uphance_headers[customer])
        common.logger.debug('Get ' + url)
    else:
        common.logger.warning('Error in api_type: ' + api_type)
        return_error = 'Error in api_type'
        return return_error, 'NULL'

    if response.status_code == 200:
        common.logger.debug('Uphance ' + api_type + ' successful for ' + customer)
        common.logger.debug(response.json())
        return return_error, response.json()  #this should be a False
    else:
        common.logger.warning('Uphance ' + api_type + ' error for ' + customer + '\nURL: ' + url + '\nResponse Status Code: ' + str(response.status_code))
        return str(response.status_code), 'NULL'
    

    #common.logger.info('Dummy API uphance call for ' + customer + '\n' + api_type + '\n' + str(url) + '\n' + str(json))
    #return True

def process_MO_file(customer,stream_id,f,data,data_lines) :
    global error

    '''
    error codes for processing and rejecting CD files
    0 - File OK and moved to 'received' folder
    1 - File Rejected and moved to 'rejected' folder
    2 - File not processed by Uphance but can still be moved to 'received folder'
    3 - File not processed by Uphance and should be left in 'pending' folder
    '''

    order_id = get_CD_parameter(data_lines,'MO',2)
    common.logger.debug('order_id: ' + order_id)
    if order_id.isnumeric():
        url = 'https://api.uphance.com/pick_tickets/' + order_id + '?service=Packing'
        #print(url)
        result = uphance_api_call(customer,'put',url=url)
        if not result[0] :
            common.send_email(0,'CD_FTP_Process_info','CD processing complete:\nStream ID:' + stream_id + '\n' + \
                                                                                   'Input File: ' + f + '\n' +
                                                                                   data +\
                                                                                   'URL: ' + url,['global'],customer=customer)
                                                                                   
            common.logger.debug('MO email sent')
        else:
            error['MO'] = result[0]
            error['File Status'] = 3
            common.logger.warning('Logging warning for ' + customer + ': MO file was not procesed by Uphance.\nHTTP Error returned = ' + str(result[0]) + '\n\nFile was not processed - will be retried at next run : FileName = ' + f + '\n\nCross Docks data:\n\n' + str(data))
    else:
        error['MO'] = True
        error['File Status'] = 1
        error['Logger Text'] = 'Unable to retrieve order number from Cross Docks info'
        common.logger.warning(customer + '\n\n' + str(error) + '\n\nFile was rejected : FileName = ' + f + '\n\nCross Docks data:\n\n' + str(data)) 

def process_PC_file(customer,stream_id,f,data,data_lines):
    global error

    '''
    error codes for processing and rejecting CD files
    0 - File OK and moved to 'received' folder
    1 - File Rejected and moved to 'rejected' folder
    2 - File not processed by Uphance but can still be moved to 'received folder'
    3 - File not processed by Uphance and should be left in 'pending' folder
    '''

    short_shipped = False
    order_id = get_CD_parameter(data_lines,'OS1',2)
    tracking = get_CD_parameter(data_lines,'OS1',19)
    carrier = get_CD_parameter(data_lines,'OS1',12)
    shipping_cost = get_CD_parameter(data_lines,'OS1',18)
    uphance_ord_no = get_CD_parameter(data_lines,'OS1',13)
    ship_to_name = get_CD_parameter(data_lines,'OS1',6)
    ship_to_address_1 = get_CD_parameter(data_lines,'OS1',7)
    ship_to_address_2 = get_CD_parameter(data_lines,'OS1',8)
    ship_to_city = get_CD_parameter(data_lines,'OS1',9)
    ship_to_state = get_CD_parameter(data_lines,'OS1',10)
    ship_to_postcode = get_CD_parameter(data_lines,'OS1',11)

    products = get_CD_parameter(data_lines,'OS2',2)
    if type(products) == str:
        products = [products]
    
    quantity_ordered = get_CD_parameter(data_lines,'OS2',3) #returns a list of quantity shipped if more than one OS2 line
    if type(quantity_ordered) == str:
        quantity_ordered = [quantity_ordered]
    
    quantity_shipped = get_CD_parameter(data_lines,'OS2',4) #returns a list of quantity shipped if more than one OS2 line
    if type(quantity_shipped) == str:
        quantity_shipped = [quantity_shipped]
    
    variance = get_CD_parameter(data_lines,'OS2',5)
    if type(variance) == str:
        variance = [variance]

    if order_id.isnumeric() :
        url = 'https://api.uphance.com/pick_tickets/'
        url = url + order_id

        #print(url)
        url_tc = url + '?' #prepare for extra info URL
        if tracking :
            url_tc = url_tc + 'tracking_number=' + tracking + '&'
        if carrier :
            if 'DHL' in carrier:
                carrier = 'dhl' #mapping to Uphance configured code for DHL
            elif ('EPARCEL' in carrier) or ('STARTRACK' in carrier) :
                carrier = 'australia_post' #mapping to Uphance configured code for Australia Post
            url_tc = url_tc + 'carrier=' + carrier + '&'
        if shipping_cost :
            url_tc = url_tc + 'shipping_cost=' + shipping_cost + '&'
        if tracking or carrier or shipping_cost :
            url_tc = url_tc[0:-1] #remove last &
            result = uphance_api_call(customer,'put',url=url_tc)
            if result[0] == '404':
                error['PC'] = result[0]
                error['Error Email Text'] = 'File Not Found (404) Error on processing information from Cross Docks - pick ticket may have been deleted after order processing has started\n\nFile moved to "received" folder'
                error['File Status'] = 2
                common.logger.warning(customer + '\n\n' + str(error))
            elif result[0]:
                error['PC'] = result[0]
                error['File Status'] = 3
                common.logger.warning(customer + ': Uphance Error while processing PC Filename: ' + f + '\n Response Error Code: ' + str(result[0]) + '\n\nFile processing will be retried at next run')

            else:
                common.logger.debug('Uphance pick ticket update successful')

        if len(error.keys()) == 0:
            if all(v == '0' for v in variance): #no variances from order in info from CD
                                                
                url_ship = url + '/ship'    
                result = uphance_api_call(customer,'put',url=url_ship) #send api call to mark status as 'ship' must be done after tracking or carrier info
                if result[0] == '404':
                    error['PC'] = result[0]
                    error['Error Email Text'] = 'File Not Found (404) Error on processing information from Cross Docks - pick ticket may have been deleted after order processing has started\n\nFile moved to "received" folder'
                    error['File Status'] = 2
                    common.logger.warning(customer + '\n\n' + str(error))   
                elif result[0]:
                    
                    error['PC'] = result[0]
                    error['File Status'] = 3
                    common.logger.warning(customer + ': Uphance Error while processing PC Filename: ' + f + '\n Response Error Code: ' + str(result[0]) + '\n\nFile processing will be retried at next run')
                else :
                    common.send_email(0,'CD_FTP_Process_info','CD processing complete:\nStream ID:' + stream_id + '\n' +
                                                                                       'Input File: ' + f + '\n' +
                                                                                       data +
                                                                                       'URL: ' + url_tc + '\n' + url_ship,['global'],customer=customer)
                    common.logger.debug('Uphance shipping update successful')
                    common.logger.debug('PC_email sent')
            else:
                variance_idx = [i for i in range(len(variance)) if variance[i] != '0']
                
                variance_table = []
                variance_table.append(["Barcode","SKU Info","Qty Ordered","Qty Shipped","Variance"])

                for i in range(len(variance_idx)):
                    url = 'https://api.uphance.com/skus?filter[ean]=' + products[variance_idx[i]] #find sku with barcode
                    result = uphance_api_call(customer,'get',url=url)
                    if not result[0]:
                        if len(result[1]['skus']) == 1 : #should only be one matching sku
                            sku = result[1]['skus'][0]
                            product_name = sku['product_name']
                            if not product_name:
                                product_name = 'N/A'
                            color = sku['color']
                            if not color:
                                color = 'N/A'
                            size = sku['size']
                            if not size:
                                size = 'N/A'
                            sku_number = sku['sku_number']
                            if not sku_number:
                                sku_number = 'N/A'
                            sku_text = 'Product: ' + product_name + ', Color: ' + color + ', Size: ' + size + ', SKU: ' + sku_number
                        else:
                            sku_text = 'Product: N/A' 
                    else :
                        sku_text = 'Product: N/A' 
                    variance_table.append([products[variance_idx[i]],sku_text,quantity_ordered[variance_idx[i]],quantity_shipped[variance_idx[i]],variance[variance_idx[i]]])

                variance_msg = tabulate(variance_table,headers = "firstrow")

                common.send_email(0,'Short Ship Response','Cross Docks are reporting that the following order was shipped without all the stock\n' + \
                                                             'The shipment has not been updated in Uphance - this will need to be done manually taking account of the stock that has not been shipped\n\n' + \
                                                             'Cross Docks file: ' + f + '\n\n' + \
                                                             'Uphance Order No: ' + str(uphance_ord_no) + '\n\n' + \
                                                             'Ship to Name: ' + str(ship_to_name) + '\n' + \
                                                             'Ship to Address 1: ' + str(ship_to_address_1) + '\n' + \
                                                             'Ship to Address 2: ' + str(ship_to_address_2) + '\n' + \
                                                             'Ship to City: ' + str(ship_to_city) + '\n' + \
                                                             'Ship to State: ' + str(ship_to_state) + '\n' + \
                                                             'Ship to Postocde: ' + str(ship_to_postcode) + '\n\n' + \
                                                             'The following items contain a shipping variance\n\n' + \
                                                             variance_msg + '\n\n',['customer','global'],customer=customer)
                                                             #'Data in CD file: \n' + data + '\n''',['global'])
                                                              
                
                common.send_email(0,'CD Short Shipped Info','CD short shipped:\nStream ID:' + stream_id + '\n' + \
                                                                               'Input File: ' + f + '\n' + \
                                                                               'Uphance Order No: ' + str(uphance_ord_no) + '\n\n' + \
                                                                               data,['global'],customer=customer)

    else:
        error['PC'] = True
        error['File Status'] = 1
        error['Logger Text'] = 'Unable to retrieve order number from Cross Docks info'
        common.logger.warning(customer + '\n\n' + str(error) + '\n\nFile was rejected : FileName = ' + f + '\n\nCross Docks data:\n\n' + str(data))
    
def process_TP_file(customer,stream_id,f,data,data_lines):
    global error 

    '''
    error codes for processing and rejecting CD files
    0 - File OK and moved to 'received' folder
    1 - File Rejected and moved to 'rejected' folder
    2 - File not processed by Uphance but can still be moved to 'received folder'
    3 - File not processed by Uphance and should be left in 'pending' folder
    '''

    po_number = get_CD_parameter(data_lines,'TP',2)
    if po_number:
        if type(po_number) == list:
            po_number = po_number[0]

        common.send_email(0,'Cross Docks Message: Purchase Order Return File Received','CD processing manual:\nStream ID: ' + stream_id + '\n' +
                                                                          'Purchase Order Number: ' + str(po_number) + '\n\n' +
                                                                           'Input File: ' + f + '\n' +
                                                                           data,['customer','global'],
                                                                           customer=customer)
        common.logger.debug('TP email sent')
    else:
        error['TP'] = True
        error['File Status'] = 1
        error['Logger Text'] = 'Unable to retrieve purchase order number from Cross Docks info'
        common.logger.warning(customer + '\n\n' + 'Failed to get Purchase Order Number from TP file. FileName = ' + f + '\n\n' + str(error))

def process_CD_file(customer,directory,f):
    global error

    '''
    error codes for processing and rejecting CD files
    0 - File OK and moved to 'received' folder
    1 - File Rejected and moved to 'rejected' folder
    2 - File not processed by Uphance but can still be moved to 'received folder'
    3 - File not processed by Uphance and should be left in 'pending' folder
    '''


    error = {}
    data = get_data_FTP(customer,directory,f)
    data_lines = data.split('\n')
    stream_id = get_CD_parameter(data_lines,'HD',3)
    common.logger.debug('stream_id: ' + stream_id)
    uphance_ord_no = None
    
    if stream_id == 'MO':  #notification that process has started in Cross Docks
        process_MO_file(customer,stream_id,f,data,data_lines)
           
    elif stream_id == 'PC' :  #confirmation of shipping by Cross Docks
        process_PC_file(customer,stream_id,f,data,data_lines)
        
    elif stream_id == 'TP' : #Purchase order return file
        process_TP_file(customer,stream_id,f,data,data_lines)
        
    elif stream_id == 'RJ' : #file rejected by Cross Docks
        error['RJ'] = True
        error['Logger Text'] = 'Cross Docks sent RJ file'
        common.logger.warning(customer + '\n\n' + 'Cross Docks sent RJ File. FileName = ' + f + '\n\n' + str(error))
        
    else:
        error['Unknown Stream ID'] = True
        error['File Status'] = 1
        error['Logger Text'] = 'Cross Docks sent unknown Stream ID in file'
        common.logger.warning(customer + '\n\n' + 'Cross Docks sent unknown Stream ID in file. FileName = ' + f + '\n\n' + str(error))
        
    if len(error.keys()) > 0 : 
        email_text = 'CD processing error :\nStream ID:' + stream_id + '\n\n'
        if 'Error Email Text' in error:
            email_text = email_text + str(error['Error Email Text']) + '\n\nOrder No: ' + str(uphance_ord_no)
            email_text = email_text + '\n\nInput File: ' + f + '\n' + data
            common.send_email(0,'CD_FTP_Process_error',email_text,['global','customer'],customer=customer)
            common.logger.debug('Error email sent')
        
        if 'File Status' in error:
            if error['File Status'] == 1:
                return 'Rejected', data
            elif error['File Status'] == 3:
                return 'Retry File', data
        
    return 'OK', data

def cross_docks_poll_request(customer):
    try:
        proc_start_time = datetime.datetime.now()

        files = get_pending_FTP_files(customer) 
        if files:
            files.sort() #sort list so that MO files are done before PC files - this helps prevent subsequent pick_ticket_update events going back to CD
            common.logger.debug('FTP files to be processed for ' + customer + ':\n' + str(files))
            proc_max_files = 100 #increased on 8th July in A.Emery Google Function - need to check timing on Google Cloud Engine
            i = 0
            proc_files = []
            rejected_files = []
            
            for f in files:
                common.logger.debug('Processing file: ' + f)
                result = process_CD_file(customer,'out/pending',f)
                if result[0] == 'OK':
                    common.logger.debug('Processing file for ' + customer + ' : ' + f)
                    if not download_file_DBX(customer,result[1],'received',f):
                        break #if get an error from Dropbox then break processing
                    if not move_CD_file_FTP(customer,'out/pending','out/sent',f):
                        break #if get an error from CD FTP then break processing
                    proc_files.append(f)
                elif result[0] == 'Rejected':
                    common.logger.debug('Processing rejected file for ' + customer + ' : ' + f)
                    if not download_file_DBX(customer,result[1],'rejected',f):
                        break #if get an error from Dropbox then break processing
                    if not move_CD_file_FTP(customer,'out/pending','out/rejected',f):
                        break #if get an error from CD FTP then break processing
                    rejected_files.append(f)
                # if not 'OK' or 'Rejected' then leave file untouched on FTP server for processing next time
                i += 1
                if i >= proc_max_files:
                    break

            proc_end_time = datetime.datetime.now()
            proc_elapsed_time = proc_end_time - proc_start_time
            proc_info_str = 'CD Files Processed :\nNum Files : ' + str(i) + '\nStart Time (UTC): ' + proc_start_time.strftime("%H:%M:%S") + '\n' + \
                            'End Time (UTC): ' + proc_end_time.strftime("%H:%M:%S") + '\n' + \
                            'Elapsed Time: ' + str(proc_elapsed_time) + '\n' + \
                            'Files Processed: ' + str(proc_files) + '\n' + \
                            'Files Rejected: ' + str(rejected_files)
            common.send_email(0,'CD Files Processed for ' + customer,proc_info_str,['global'],customer=customer)
        else:
            common.logger.debug('No files to process for ' + customer)
            proc_end_time = datetime.datetime.now()
            proc_elapsed_time = proc_end_time - proc_start_time
            
            common.send_email(0,'CD Files Processed','No files processed\nElapsed Time: ' + str(proc_elapsed_time),'gary@mclarenwilliams.com.au',customer=customer)

        return 200

    except Exception as e:
        common.logger.exception('Exception message for : ' + customer + '\nError in Cross Docks Polling:\nException Info: ' + str(e))
        return 500
    
    
