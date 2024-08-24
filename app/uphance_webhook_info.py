import os
import pandas as pd
#import dropbox
from collections import defaultdict
from datetime import datetime
from dateutil import tz
import importlib
import ftputil

#include general files

import FlaskApp.app.file_format_GMcL  as file_format_GMcL #default configuration data for Cross Dock formatting -> see Jupyter Notebook for code to create this info from Cross Docks formatting spreadsheet
import FlaskApp.app.common as common

custom_file_format_modules = {}

for c in common.customers :
    custom_file_format_modules[c] = importlib.import_module('FlaskApp.app.file_format_'+ c)


from_zone = tz.tzutc()
to_zone = tz.tzlocal()

process_webhook_depth = 0

cc_codes_pd = pd.read_csv('/var/www/FlaskApp/FlaskApp/app/CountryCodes.csv',index_col='Country')


def processQueuedFiles(customer,folder):
    global error,request_dict

    queuedFiles = common.getLocalFiles(folder,customer=customer,error=error,request_dict=request_dict)
    if queuedFiles[0] : #only process files if no errors on getting Local files
        for file_item in queuedFiles[1]:
            if transfer_FTP(customer,file_item['file_name'],file_item['file_data'],True): #flag this is a retry to avoid another saving on error
                os.remove(os.path.join(folder,file_item['file_name'])) #remove file if FTP successful
                common.logger.info('Logger Info for ' + customer + '\nLocal file successully transferred via FTP and removed locally\nFile: ' + file_item['file_name'])

def transfer_FTP(customer,file_name,file_data,retry=False):
    global error, request_dict

    cross_docks_info = common.get_CD_FTP_credentials(customer)
    try: 
        #code for testing only
        #raise Exception("Testing error in FTP transfer")
        #end of test code

        with ftputil.FTPHost("ftp.crossdocks.com.au", cross_docks_info['username'], cross_docks_info['password']) as ftp_host:
            common.logger.debug('CD credentials : '+ cross_docks_info['username'] + ':' + cross_docks_info['password'])
            common.logger.debug('CD getcwd : ' + ftp_host.getcwd())
            ftp_host.chdir('in/pending')
            with ftp_host.open(file_name, "w", encoding="utf8") as fobj:
                fobj.write(file_data)
    
    except Exception as ex:
        
        common.logger.warning('Logging Warning Error for :' + customer + '\nUphance_webhook_error','Cross Docks FTP Error - need to check if file sent to Cross Docks\nFile Name: ' + file_name + '\nError Info: ' + str(error) + '\nFTP Error:' + str(ex) + 'Output file:\n' + file_data + '\nInput Request:\n' + str(request_dict),['global'])
        error['send_to_CD'] = False;
        if not retry:
            common.storeLocalFile(os.path.join('home/gary/cd_send_files',customer),file_name,file_data,customer=customer,error=error,request_dict=request_dict)  #store file locally
            common.logger.info('Logging Info for ' + customer + "\nFile " + file_name + ' stored locally')
        return False
        
    common.logger.debug('Logging Info for ' + customer + "\nFile " + file_name + ' sent to FTP server')

    return True
    

def get_custom_file_format(customer,stream_id,ri):
    common.logger.debug(custom_file_format_modules[customer].CD_file_format[stream_id][ri])

    return  custom_file_format_modules[customer].CD_file_format.get(stream_id,{}).get(ri,{}).get('mapping',{})  # see https://stackoverflow.com/questions/26979046/python-check-multi-level-dict-key-existence

def create_field_line(field_str,field_list,mapping):
    #print(kwargs)
    field_values = {}
    for f in field_list:
        if f in mapping:
            if type(mapping[f]) == str:
                field_values[f] = " ".join(mapping[f].split()) #this removes any new lines, tabs etc and replaces with " "
            else:
                field_values[f] = mapping[f]
        else:
            field_values[f] = ''
    field_line = field_str.format(data = field_values) + '\n'
    #print(field_line)
    return field_line

def getNotesLength(notes_list):
    return len(notes_list) - notes_list.count('')

def getQtyOrdered(event_data,index1,index2):
    qty_ord = int(event_data['line_items'][index1]['line_quantities'][index2]['quantity'])
    if qty_ord > 0:
        return (qty_ord)
    else:
        return None

def checkAddressForError(event_data):
    
    address_error = {}
    
    if event_data['address']['country'] in cc_codes_pd['Alpha-2 code'].to_list() : #country codes with 2 letters
        country = cc_codes_pd.index[cc_codes_pd['Alpha-2 code'] == event_data['address']['country']].to_list()[0]
    else:
        country = event_data['address']['country']
    if country == 'Australia' :
        if event_data['address']['state'].upper() not in ['NSW','VIC','QLD','WA','SA','TAS','ACT','NT'] :
            address_error['Aust State Error'] = 'Not in List of Abbreviations'
        if len(event_data['address']['postcode']) != 4:
            address_error['Aust Postcode Error'] = 'Wrong Length'
    elif not country:
        address_error['Country Error'] = 'No Country'
    if not event_data['address']['city'] :
        address_error['City Error'] = 'No City'
    
    return address_error

def process_record_indicator(customer,event_data,stream_id,ri,mapping):
    global error,mapping_code #so that can be displayed in exception logging

    loop_lengths_known = False
    loops = {} #use dictionary for values in exec() function as simple variables don't seem to be accessible
    data = {} 
    
    #print(mapping,list(mapping)[0])
    loops = mapping[list(mapping)[0]]['Loops'][0]  #get Loop value for first key in mapping dict
    if int(loops) == 0 : #no loops
        for ci in mapping.keys():
            mapping_code = mapping[ci]['Processing'][0].format_map(defaultdict(str,var='event_data')) 
            #print(mapping_code)
            exec(mapping_code)
            #print('data[ci]',data[ci])    
        if len(error.keys()) > 0:
            common.logger.info('\nLogger Info for ' + customer + '\nError info:' + stream_id + '\n' + str(error) + '\n' + str(event_data))
        return create_field_line(file_format_GMcL.CD_file_format[stream_id][ri]['template'],file_format_GMcL.CD_file_format[stream_id][ri]['Col List'],data)
    else: #process loops
        loops_dict = {}
        ci = list(mapping)[0] #get loop value for first in key in mapping dict
        #print(mapping,'\n',ci)
        mapping_code = mapping[ci]['Loop_Len1'][0].format_map(defaultdict(str,var='event_data'))
        #print(ri,mapping_code)
        #print(mapping[ci]['Loop_Len1'][0].format_map(defaultdict(str,var='event_data')))
        exec(mapping_code)
        #print('loops_dict',loops_dict)
        line_count = 0
        multi_line = ''

        for i1 in range(loops_dict[1]):
            #print(ci,mapping[ci])
            if int(loops) == 2 :
                mapping_code = mapping[ci]['Loop_Len2'][0].format_map(defaultdict(str,var='event_data',index1 = i1))
                exec(mapping_code)
                #print('loops_dict',loops_dict)
                #print('loops',loops)
                for i2 in range(loops_dict[2]):
                    #data = {}
                    blank_line = {0:False}  #for some reason only dicts are static through the exec() function
                    line_count += 1
                    for ci in mapping.keys():
                        mapping_code =  mapping[ci]['Processing'][0].format_map(defaultdict(str,var='event_data',index1=i1,index2=i2,line_count=line_count))
                        #print(mapping_code)
                        exec(mapping_code)
                        #print("data[ci]",ci,data[ci])
                        #print('Mapping Code',mapping_code)
                        #print('Data:',ci,blank_line,data[ci])
                        if blank_line[0]:
                            #print('blank line')
                            line_count -= 1 #need to decrement line_count 
                            break
                    if not blank_line[0]: #blank line set in code for field
                        multi_line = multi_line + create_field_line(file_format_GMcL.CD_file_format[stream_id][ri]['template'],file_format_GMcL.CD_file_format[stream_id][ri]['Col List'],data)
            elif int(loops) == 1:
                #data = {}
                blank_line = {0:False} #for some reason only dicts are static through the exec() function
                line_count += 1
                for ci in mapping.keys():
                    mapping_code = mapping[ci]['Processing'][0].format_map(defaultdict(str,var='event_data',index1=i1,line_count=line_count))
                    #print(ci,mapping_code)
                    exec(mapping_code)
                    #print("data[ci]",ci,data[ci])
                    if blank_line[0]:
                        line_count -= 1 #need to decrement line_count 
                        break
                if not blank_line[0]: #blank line set in code for field
                    multi_line = multi_line + create_field_line(file_format_GMcL.CD_file_format[stream_id][ri]['template'],file_format_GMcL.CD_file_format[stream_id][ri]['Col List'],data)
        if len(error.keys()) > 0:
            common.logger.debug('\nLogger Info for ' + customer + '\nError info:' + stream_id + '\n' + str(error) + '\n' + str(event_data))  #downgraded to debugging on 25 July 2024 to reduce email load 
        return multi_line
    
def process_all_record_indicators(customer,event_data,stream_id):
    
    loops = {} #use dictionary for values in exec() function as simple variables don't seem to be accessible
    record_indicators = file_format_GMcL.CD_file_format[stream_id].keys()
    file_data = ''

    for ri in record_indicators :
        #data = {}
        mapping = file_format_GMcL.CD_file_format[stream_id][ri]['mapping']
        new_file_data = process_record_indicator(customer,event_data,stream_id,ri,mapping)
        mapping = get_custom_file_format(customer,stream_id,ri) #get any custom mapping and override default if that is the case
        common.logger.debug('Logger Info for : ' + customer + '\nCustom Mapping Code for Stream ID : ' + str(stream_id) + '\nRecord Indicator :' + str(ri) + '\nMapping : ' + str(mapping))
        if len(mapping.keys()) > 0:
            if mapping['RECORD_INDICATOR']['Processing'][0] : 
                new_file_data = process_record_indicator(customer,event_data,stream_id,ri,mapping)
                common.logger.debug('Logger Info for : ' + customer + '\nCustom File Data for Stream ID : ' + str(stream_id) + '\nRecord Indicator :' + str(ri) + '\nMapping : ' + str(mapping) + '\nNew File Data : ' + new_file_data)
        file_data = file_data + new_file_data
    #print(file_data)
    return file_data
    #print('Error info2:',error,error.keys(),len(error.keys()))
    
def process_file(customer,file_data,file_name):
    global error, stream_id, request_dict

    dbx_file = common.access_secret_version('customer_parameters',customer,'dbx_folder') + '/sent/' + file_name

    if not common.store_dropbox(customer,file_data,dbx_file):
        common.logger.warning('Cross Docks file not stored in Dropbox - processing has continued\nFile Name: ' + file_name + '\nFile Contents : \n' + file_data)
    
    '''move to common.py
    #below exception handling implemented 2024-08-09 to cope with intermittent dropbox errors
    try:
        with io.BytesIO(file_data.encode()) as stream:
            stream.seek(0)

            common.dbx.files_upload(stream.read(), dbx_file, mode=common.dropbox.files.WriteMode.overwrite)

        common.logger.debug('Dropbox Transferred Successfully')

    

    except Exception as ex:
        common.logger.warning('Logging Warning Error for :' + customer + '\nUphance_webhook_error','Uphance Dropbox Error - need to check if file sent to Dropbox\nFile Name: ' + file_name + '\nError Info: ' + str(error) + '\nDropbox Error:' + str(ex) + 'Output file:\n' + file_data + '\nInput Request:\n' + str(request_dict),['global'])
        
        common.logger.debug('Dropbox Transfer Error')

    '''

    if len(error.keys()) == 0 : #no errors reported so send to Cross Docks
        if transfer_FTP(customer,file_name,file_data):
            common.logger.debug('transfer_FTP ok after no error')
            #process any stored files since last FTP was successful so can resend queued files to Cross Docks
            processQueuedFiles(customer,os.path.join('home/gary/cd_send_files',customer))
            
        else:
            error['FTP transfer error'] = 'Error in  transfer of file: ' + file_name
            common.logger.warning('transfer_FTP error for file: ' + file_name +'\nFile should have been stored on server for sending to Cross Docks when FTP up again')
    elif 'send_to_CD' in error :
        if error['send_to_CD'] :
            if transfer_FTP(customer,file_name,file_data):
                common.logger.debug('transfer_FTP ok after error: ' + str(error))
                #process any stored files since last FTP was successful so can resend queued files to Cross Docks
                processQueuedFiles(customer,os.path.join('home/gary/cd_send_files',customer))
            else:
                error['FTP transfer error'] = 'Error in  transfer of file: ' + file_name
                common.logger.warning('transfer_FTP error for file: ' + file_name +'\nFile should have been stored on server for sending to Cross Docks when FTP up again')
        

    
def process_pick_ticket(customer,event_data):
    global error, stream_id
    if event_data['service'] != 'Packing':
        stream_id = 'OR'
        event_id = event_data['id']
        event_shipment_number = event_data['shipment_number']
        event_date = str(datetime.strptime(event_data['updated_at'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=from_zone).astimezone(to_zone).strftime("%Y%m%dT%H%M%S"))

        file_data = process_all_record_indicators(customer,event_data,stream_id)
        file_name = stream_id + event_date + '_' + str(event_id).zfill(4) + '_' + str(event_shipment_number).zfill(4) + '.csv'
        process_file(customer,file_data,file_name)
        #if common.data_store[customer]:   no longer need as polling dropbox files
        #    common.storeLocalFile(os.path.join('home/gary/data_store',customer),file_name,file_data,customer=customer,error=error,request_dict=request_dict)
        return file_data
    else:
        return "Not Sent to Cross Docks - Already in Packing State"

def process_pick_ticket_delete(customer,event_data):
    global error, stream_id
    #need to send to CD regardless of packing state as unable to check state as pick ticket has been deleted.
    event_id = event_data['id']
    
    stream_id = 'OR'
    event_id = event_data['id']

    CD_code = common.access_secret_version('customer_parameters',customer,'CD_customer_code')

    file_data = 'HD|'+CD_code+'|OR\nOR1|D|' + str(event_id) + '\n'#special short code to delete order before packing
    file_name = stream_id + datetime.now().strftime("%Y%m%dT%H%M%S") + '_' + str(event_id).zfill(4) + '_' + 'pick_ticket_delete' + '.csv'
    process_file(customer,file_data,file_name)
    return file_data

def process_product_update(customer,event_data):
    global error, stream_id
    stream_id = 'IT'
    event_id = event_data['id']
    event_date = str(datetime.now().strftime("%Y%m%dT%H%M%S"))
    event_name = event_data['name']
    file_data = process_all_record_indicators(customer,event_data,stream_id)
    file_name = stream_id + event_date + '_' + str(event_id) + '_' + event_name.replace('/','_').replace(' ','_') + '.csv'
    if len(file_data.split('\n')) > 2 : #then not an empty CD file
        process_file(customer,file_data,file_name)
    else:
        error['send_to_CD'] = False #override any error['send_to_CD'] to correct error messages at end of processing
        common.logger.debug('\nLogger Info for ' + customer + '\nFile not sent to CD as no IT records\n' + file_data + '\n' + str(event_data)) #downgraded to debugging on 25 July 2024 to reduce email load 
    
    return file_data

def process_production_order(customer,event_data):
    global error, stream_id
    if event_data['status'] != 'checked in':
        stream_id = 'PT'
        event_id = str(event_data['production_order_number'])
        event_date = str(datetime.strptime(event_data['updated_at'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=from_zone).astimezone(to_zone).strftime("%Y%m%dT%H%M%S"))
        event_name = event_data['vendor'] + '_' + event_data['delivery_name']
        file_data = process_all_record_indicators(customer,event_data,stream_id)
        file_name = stream_id + event_date + '_' + str(event_id) +'_' + event_name.replace('/','_').replace(' ','_') + '.csv'
        process_file(customer,file_data,file_name)
        return file_data
    else:
        return "Not Sent to Cross Docks - Already Checked In"

def process_production_order_delete(customer,event_data):
    global error, stream_id
    #need to send to CD regardless of packing state as unable to check state as pick ticket has been deleted.
    event_id = event_data['id']
    
    stream_id = 'PT'
    event_id = event_data['id']

    CD_code = common.access_secret_version('customer_parameters',customer,'CD_customer_code')

    file_data = 'HD|'+CD_code+'|PT\nPT1|D|' + str(event_id) + '\n'#special short code to delete order before packing
    file_name = stream_id + datetime.now().strftime("%Y%m%dT%H%M%S") + '_' + str(event_id).zfill(4) + '_' + 'receiving_ticket_delete' + '.csv'
    process_file(customer,file_data,file_name)
    return file_data

def process_uphance_event(customer,event_dict) :
    global error
    error = {}

    if (event_dict['event'] == 'pick_ticket_create') or (event_dict['event'] == 'pick_ticket_update'):
        return process_pick_ticket(customer,event_dict['pick_ticket'])
    
    elif (event_dict['event'] == 'pick_ticket_delete'):
        return process_pick_ticket_delete(customer,event_dict['pick_ticket'])

    elif (event_dict['event'] == 'product_create') or (event_dict['event'] == 'product_update'):
        return process_product_update(customer,event_dict['product'])   
    
    elif event_dict['event'] == 'receiving_ticket_update':
        return process_production_order(customer,event_dict['receiving_ticket'])
    
    elif event_dict['event'] == 'receiving_ticket_create':
        return process_production_order(customer,event_dict['receiving_ticket']) 

    elif event_dict['event'] == 'receiving_ticket_delete':
        return process_production_order_delete(customer,event_dict['receiving_ticket'])    
        
    return "NULL"

def remove_special_unicode_chars(obj):
    if isinstance(obj, dict):
        return {remove_special_unicode_chars(key): remove_special_unicode_chars(value) for key, value in obj.items()}
    elif isinstance(obj, str):
        return obj.encode('ascii', 'ignore').decode('utf-8')
    elif obj is None:  #if receive a None then convert to empty string
        return ''
    return obj
            

def uphance_process_webhook(customer,request):
    global dbx, mapping_code, stream_id, request_dict,error, process_webhook_depth
    # Extract relevant data from the request payload

    mapping_code = ''
    stream_id = ''
    process_webhook_depth += 1
    
    ''' Code for multiple attempts if exception occurs - but not used at the moment - 2024-08-09 
    if process_webhook_depth > 1 : 
        common.logger.info('Exception message for : ' + customer + '\nError in Uphance Process Webhook:\nProcess Webhook Depth = ' + str(process_webhook_depth))
    if process_webhook_depth > 2 :
        common.logger.exception('Exception message for : ' + customer + '\nError in Uphance Process Webhook:\nProcess Webhook Depth Limit reached') #don't keep retrying
        return False 
    '''

    try:
        if type(request) != dict:
            request_dict = request.get_json()
        else:
            request_dict = request  #used for test purposes when request is a dict
        ##remove any special characters from the webhook from Uphhance
        request_dict = remove_special_unicode_chars(request_dict)
        data_str = process_uphance_event(customer,request_dict)
        if len(error.keys()) == 0:
            common.send_email(0,'Uphance_webhook_info','Uphance processing complete:\nOutput file:\n' + data_str + '\nInput Request:\n' + str(request_dict),['global'],customer=customer)
            return True #successful
        else:
            sendees = ['global'] #default to only global email recipients
            for filter_text in common.access_secret_version('customer_parameters',customer,'errors_to_be_reported'):
                if any(filter_text in string for string in error.keys()):
                    sendees.append('customer')
            common.logger.debug('Sending error report to : ' + str(sendees)) 
            if error['send_to_CD']:
                error_message = 'There was an error when processing information received from Uphance - however the file was still sent to Cross Docks' 
            else:
               error_message = 'There was an error when processing information received from Uphance - the file was not sent to Cross Docks' 
            common.send_email(0,'Error processing Uphance webhook',error_message + '\n\nError Info: ' + str(error) + '\n' + 'Output file:\n' + data_str + '\nInput Request:\n' + str(request_dict),sendees,customer=customer)
        
        #common.logger.debug('Uphance Sub Process return True')
        process_webhook_depth = 0 #decrement
        return True #successful
        
    except Exception as e:
        common.logger.exception('Exception message for : ' + customer + '\nError in Uphance Process Webhook:\nStream ID : ' + str(stream_id) + '\nMapping Code :\n' + str(mapping_code) + '\nRequest:\n' + str(request_dict) + '\nException Info: ' + str(e))
        #common.logger.debug('Uphance Sub Process return False')
        
        ''' Code for multiple attempts if exception occurs - but not used at the moment - 2024-08-09 
        uphance_process_webhook(customer,request) : #try webhook processing again in case of intermittent Dropbox or FTP error
        if process_webhook_depth == 0:
            return True
        else:
            return False #error '''

        return False

def uphance_prod_webhook(customer,request):
    #assume all good to respond with HTTP 200 response
    #x = threading.Thread (target = uphance_process_webhook,args=(request,))
    #x.start()
    #common.initialise_exception_logging()
    common.logger.debug(customer + '\n' + str(request))
    if uphance_process_webhook(customer,request):
        #common.logger.debug('Uphance Process return True')
        return 200  
    else:
        #common.logger.debug('Uphance Process return False')
        return 500  #return HTTP 500 response - Internal Server Error - hopefully Uphance will retry webhook


