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
                common.send_email(customer,0,'CD_FTP_Rejected_Files','Rejected Files reported by Cross Docks:\n' + str(rejected_files),['global'])
    
    except Exception as ex:
        common.logger.warning('Cross Docks Error on getting pending files for ' + customer + '\nException : ' + str(ex))
        return False

    common.logger.debug('Files returned for ' + customer + 'from FTP curdir:' + str(pending_files))    
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


def download_file_DBX(customer,file_data,file):
    
    dbx_file = common.access_secret_version('customer_parameters',customer,'dbx_folder') + '/received/' + file
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
        return False

    if response.status_code == 200:
        common.logger.debug('Uphance ' + api_type + ' successful for ' + customer)
        common.logger.debug(response.json())
        return response.json()
    else:
        common.logger.warning('Uphance ' + api_type + ' error for ' + customer)
        common.logger.warning(response.status_code)
        return False
    

    #common.logger.info('Dummy API uphance call for ' + customer + '\n' + api_type + '\n' + str(url) + '\n' + str(json))
    #return True

    
def process_CD_file(customer,directory,f):
    error = False
    data = get_data_FTP(customer,directory,f)
    data_lines = data.split('\n')
    stream_id = get_CD_parameter(data_lines,'HD',3)
    common.logger.debug('stream_id: ' + stream_id)
    
    if stream_id == 'MO':  #notification that process has started in Cross Docks
        order_id = get_CD_parameter(data_lines,'MO',2)
        common.logger.debug('order_id: ' + order_id)
        if order_id:
            url = 'https://api.uphance.com/pick_tickets/' + order_id + '?service=Packing'
            #print(url)
            if uphance_api_call(customer,'put',url=url) :
                common.send_email(customer,0,'CD_FTP_Process_info','CD processing complete:\nStream ID:' + stream_id + '\n' + \
                                                                                       'Input File: ' + f + '\n' +
                                                                                       data +\
                                                                                       'URL: ' + url,['global'])
                common.logger.debug('MO email sent')
            else:
                error = True
        else:
            error = True
            
    elif stream_id == 'PC' :  #confirmation of shipping by Cross Docks
        short_shipped = False
        order_id = get_CD_parameter(data_lines,'OS1',2)
        tracking = get_CD_parameter(data_lines,'OS1',19)
        carrier = get_CD_parameter(data_lines,'OS1',12)
        
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

        if order_id :
            url = 'https://api.uphance.com/pick_tickets/'
            url = url + order_id

            if all(v == '0' for v in variance): #no variances from order in info from CD
                
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
                if tracking or carrier :
                    url_tc = url_tc[0:-1] #remove last &
                    if not uphance_api_call(customer,'put',url=url_tc):
                        error = True                                           
                if not error :
                    url_ship = url + '/ship'    
                    if not uphance_api_call(customer,'put',url=url_ship): #send api call to mark status as 'ship' must be done after tracking or carrier info
                        error = True
                    if not error :
                        common.send_email(customer,0,'CD_FTP_Process_info','CD processing complete:\nStream ID:' + stream_id + '\n' +
                                                                                           'Input File: ' + f + '\n' +
                                                                                           data +
                                                                                           'URL: ' + url_tc + '\n' + url_ship,['global'])
                        common.logger.debug('PC_email sent')
            else:
                variance_idx = [i for i in range(len(variance)) if variance[i] != 0]

                variance_table = []
                variance_table[0] = ["Barcode","Qty Ordered","Qty Shipped","Variance"]
                for i in range(len(variance_idx)):
                    variance_table.append([products[variance_idx[i]],quantity_ordered[variance_idx[i]],quantity_shipped[variance_idx[i]],variance[variance_idx[i]]])

                variance_msg = tabulate(variance_table)

                common.send_email(customer,0,'Cross Docks Message: Short Ship Response','Cross Docks are reporting that the following order was shipped without all the stock\n' + \
                                                             'The shipment has not been updated in Uphance - this will need to be done manually taking account of the stock that has not been shipped\n' + \
                                                             'Cross Docks file: ' + f + '\n' + \
                                                             variance_msg + '\n\n' + \
                                                             'Data in CD file: \n' + data + '\n',['global'])
                                                              
                
                common.send_email(customer,0,'CD_Short_Shipped','CD short shipped:\nStream ID:' + stream_id + '\n' +
                                                                               'Input File: ' + f + '\n' +
                                                                               data,['global'])

        else:
            error = True
    
    
    elif stream_id == 'TP' : #Purchase order return file
        po_number = get_CD_parameter(data_lines,'TP',2)
        if po_number:
            if type(po_number) == list:
                po_number = po_number[0]

            common.send_email(customer,0,'Cross Docks Message: Purchase Order Return File Received','CD processing manual:\nStream ID: ' + stream_id + '\n' +
                                                                              'Purchase Order Number: ' + str(po_number) + '\n\n' +
                                                                               'Input File: ' + f + '\n' +
                                                                               data,['customer','global'])
            common.logger.debug('TP email sent')
        else:
            common.logger.warning('Failed to get Purchase Order Number from TP file. FileName = ' + f)
            

    
    elif stream_id == 'RJ' : #file rejected by Cross Docks
        error = True
        
    else:
        error = True
        
    if error : 
        common.send_email(customer,0,'CD_FTP_Process_error','CD processing error (check Google Cloud logs):\nStream ID:' + stream_id + '\n' +
                                                                               'Input File: ' + f + '\n' +
                                                                               data,['global'])
        common.logger.debug('Error email sent')
        return False #flag error
        
    return data

def cross_docks_poll_FTP(customer):
    try:
        proc_start_time = datetime.datetime.now()

        files = get_pending_FTP_files(customer) 
        if files:
            files.sort() #sort list so that MO files are done before PC files - this helps prevent subsequent pick_ticket_update events going back to CD
            common.logger.debug('FTP files to be processed for ' + customer + ':\n' + str(files))
            proc_max_files = 100 #increased on 8th July in A.Emery Google Function - need to check timing on Google Cloud Engine
            i = 0
            proc_files = []
            
            for f in files:
                common.logger.debug('Processing file: ' + f)
                result = process_CD_file(customer,'out/pending',f)
                if result:
                    common.logger.debug('Processing file for ' + customer + ' : ' + f)
                    if not download_file_DBX(customer,result,f):
                        break #if get an error from Dropbox then break processing
                    if not move_CD_file_FTP(customer,'out/pending','out/sent',f):
                        break #if get an error from CD FTP then break processing
                    proc_files.append(f)
                i += 1
                if i >= proc_max_files:
                    break

            proc_end_time = datetime.datetime.now()
            proc_elapsed_time = proc_end_time - proc_start_time
            proc_info_str = 'CD Files Processed :\nNum Files : ' + str(i) + '\nStart Time (UTC): ' + proc_start_time.strftime("%H:%M:%S") + '\n' + \
                            'End Time (UTC): ' + proc_end_time.strftime("%H:%M:%S") + '\n' + \
                            'Elapsed Time: ' + str(proc_elapsed_time) + '\n' + \
                            'Files Processed: ' + str(proc_files)
            common.send_email(customer,0,'CD Files Processed for ' + customer,proc_info_str,['global'])
        else:
            common.logger.debug('No files to process for ' + customer)
            proc_end_time = datetime.datetime.now()
            proc_elapsed_time = proc_end_time - proc_start_time
            
            common.send_email(customer,0,'CD Files Processed','No files processed\nElapsed Time: ' + str(proc_elapsed_time),'gary@mclarenwilliams.com.au')
    except Exception as e:
        common.logger.exception('Exception message for : ' + customer + '\nError in Cross Docks Polling:\nException Info: ' + str(e))
    
    
    
def cross_docks_poll_request(customer):
    cross_docks_poll_FTP(customer)
    
