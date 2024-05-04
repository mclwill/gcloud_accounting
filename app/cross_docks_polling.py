import functions_framework
import json
#import smtplib
#import logging
#import logging.handlers
#import threading
#import pandas as pd
#import dropbox
import io
#import os
#from collections import defaultdict
import datetime
from dateutil import tz
import ftputil
#import time
#import random
import requests
import traceback

#import local files
#import uphance_credentials #information to login into uphance - will expire after one year - ie. Feb 2025
import common


from_zone = tz.tzutc()
to_zone = tz.tzlocal()

'''smtp_handler = logging.handlers.SMTPHandler(mailhost=('smtp.gmail.com', 587),
                                            fromaddr="zd_zapier@mclarenwilliams.com.au", 
                                            toaddrs="gary@mclarenwilliams.com.au",
                                            subject=u"Uphance webhook info logging exception",
                                            credentials=('zd_zapier@mclarenwilliams.com.au', 'yEc9m3G9f?ATeJtF'),
                                            secure=())

logger = logging.getLogger()
logger.addHandler(smtp_handler)

aemery_dbx_refresh_token = 'tX62hzYj2h0AAAAAAAAAATd7DM2a0yAnNSW_P7SpMx_LA3JV4QnufbN-ddRDt0cd'
aemery_dbx_app_key = 'uxrn09slklm1una'
aemery_dbx_app_secret = 'in08qrhgq0qit5n'

cross_docks_username = "ftpemprod"
cross_docks_pw = "ftp3m#2024"'''

dbx_folder = "/A.Emery/Wholesale/APIs (Anna's Dad)/Cross Docks Info/"

'''def send_email(message_subject,message_text,receiver_email_address):
    sender_email = 'zd_zapier@mclarenwilliams.com.au'
    sender_pw = 'yEc9m3G9f?ATeJtF'
    try: 
        #Create your SMTP session 
        smtp = smtplib.SMTP('smtp.gmail.com', 587) 

       #Use TLS to add security 
        smtp.starttls() 

        #User Authentication 
        smtp.login(sender_email,sender_pw)

        if type(receiver_email_address) == str:
            receiver_email_address = [receiver_email_address]

        #Defining The Message 
        message = "From: aemery_gcf <zd_zapier@mclarenwilliams.com.au>\nTo:  %s\r\n" % ",".join(receiver_email_address) + 'Subject: ' + message_subject + '\n\n' + message_text

        #Sending the Email
        smtp.sendmail(sender_email, receiver_email_address,message.encode('utf-8')) 

        #Terminating the session 
        smtp.quit() 
        return True

    except SMTPResponseException as ex:
        error_code = ex.smtp_code
        error_message = ex.smtp_error
        logger.exception(ex)
        if error_code == 421 : #try again after random time interval up to 5 seconds
            time.sleep(random.random(5))
            send_email(message_subject,message_text,receiver_email_address)
            return True
'''    
def get_pending_FTP_files():
    try: 
        with ftputil.FTPHost("ftp.crossdocks.com.au", common.cross_docks_username, common.cross_docks_pw) as ftp_host:

            ftp_host.chdir('out/pending')
            pending_files = ftp_host.listdir(ftp_host.curdir)

            ftp_host.chdir('../../in/rejected')  #check for any rejected files
            rejected_files = ftp_host.listdir(ftp_host.curdir)

            if rejected_files:
                common.send_email(0,'CD_FTP_Rejected_Files','Rejected Files reported by Cross Docks:\n' + str(rejected_files),"gary@mclarenwilliams.com.au")
    
    except Exception as ex:
        print(ex)
        common.send_email(0,'CD_FTP_Error','Cross Docks Error on getting pending files\nException : ' + str(ex),"gary@mclarenwilliams.com.au")
        raise

    print('Files returned from FTP curdir:',pending_files)    
    return pending_files

def get_data_FTP(directory,f):
    try: 
        with ftputil.FTPHost("ftp.crossdocks.com.au", common.cross_docks_username, common.cross_docks_pw) as ftp_host:

            ftp_host.chdir(directory)
            with ftp_host.open(f,'r') as fobj:
                data = fobj.read()
    
    except Exception as ex:
        print('Error in get_data_FTP:',ex)
        raise
        
    return data

def move_CD_file_FTP(source,dest,f):
    try: 
        with ftputil.FTPHost("ftp.crossdocks.com.au", common.cross_docks_username, common.cross_docks_pw) as ftp_host:
            with ftp_host.open(source + '/' + f,'rb') as source_obj:
                with ftp_host.open(dest + '/' + f,'wb') as dest_obj:
                    ftp_host.copyfileobj(source_obj,dest_obj)
                    ftp_host.remove(source + '/' + f)
                    
    
    except Exception as ex:
        print('Error in move_CD_file_FTP:',ex)
        raise
        
    return True

def transfer_FTP(file_name,file_data):
    
    try: 
        with ftputil.FTPHost("ftp.crossdocks.com.au", common.cross_docks_username, common.cross_docks_pw) as ftp_host:

            ftp_host.chdir('in/pending')
            with ftp_host.open(file_name, "w", encoding="utf8") as fobj:
                fobj.write(file_data)
    
    except Exception as ex:
        print('Error in transfer_FTP:',ex)
        raise
    
    #print("File " + file_name + 'sent to FTP server')
    
'''def dropbox_initiate():
    global dbx
    dbx = dropbox.Dropbox(app_key=aemery_dbx_app_key,app_secret=aemery_dbx_app_secret,oauth2_refresh_token=aemery_dbx_refresh_token)
'''

def download_file_DBX(file_data,file):
    #global dbx_file
    #dbx = dropbox.Dropbox(app_key=aemery_dbx_app_key,app_secret=aemery_dbx_app_secret,oauth2_refresh_token=aemery_dbx_refresh_token)
    dbx_file = dbx_folder + 'FTP_production_files/received/' + file
    try:
        with io.BytesIO(file_data.encode()) as stream:
            stream.seek(0)
            common.dbx.files_upload(stream.read(), dbx_file, mode=common.dropbox.files.WriteMode.overwrite)

        return True
    except Exception as ex:
        print('DBX error:',ex)
        tb = traceback.format_exc()
        common.send_email(0,'DBX Error in CD Processing','Exception: ' + str(ex) + '\nTraceback:\n' + tb,'gary@mclarenwilliams.com.au')
        return False
    
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
    
'''def uphance_initiate():
    global uphance_headers
    uphance_headers = {'Authorization': 'Bearer '+ uphance_credentials.credentials['access_token'],
                   'Content-Type': 'application/json'}
    uphance_register_url = 'https://api.uphance.com/organisations/set_current_org'
    uphance_register = {'organizationId': uphance_credentials.organization_id}
    response = requests.post(uphance_register_url,json = uphance_register,headers = uphance_headers)
    
    if response.status_code == 201:
        print('Uphance initiated')
        print(response.json())
        return True
    else:
        print(response.status_code)
        return False
'''

def uphance_api_call(api_type,**kwargs):
    url = kwargs.pop('url',None)
    json = kwargs.pop('json',None)
    
    if api_type == 'post':
        response = requests.post(url,json = json,headers = common.uphance_headers)
        print('Post', url)
    elif api_type == 'put':
        response = requests.put(url,headers = common.uphance_headers)
        print('Put',url)
    elif api_type == 'get' :
        response = requests.get(url,headers = common.uphance_headers)
        print('Get',url)
    else:
        print('Error in api_type:',api_type)
        return False

    if response.status_code == 200:
        print('Uphance ' + api_type + ' successful')
        print(response.json())
        return response.json()
    else:
        print('Uphance ' + api_type + ' error')
        print(response.status_code)
        return False
    
def process_CD_file(directory,f):
    error = False
    data = get_data_FTP(directory,f)
    data_lines = data.split('\n')
    stream_id = get_CD_parameter(data_lines,'HD',3)
    print(stream_id)
    
    if stream_id == 'MO':  #notification that process has started in Cross Docks
        order_id = get_CD_parameter(data_lines,'MO',2)
        print(order_id)
        if order_id:
            url = 'https://api.uphance.com/pick_tickets/' + order_id + '?service=Packing'
            #print(url)
            if uphance_api_call('put',url=url) :
                common.send_email(0,'CD_FTP_Process_info','CD processing complete:\nStream ID:' + stream_id + '\n' + \
                                                                                       'Input File: ' + f + '\n' +
                                                                                       data +\
                                                                                       'URL: ' + url,"gary@mclarenwilliams.com.au")
                print('MO_email sent')
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

        '''if type(quantity_shipped) == list:
            quantity_shipped = [int(x) for x in quantity_shipped]
            quantity_shipped = sum(quantity_shipped)
        else:
            quantity_shipped = int(quantity_shipped)'''

        print('Tracking:',tracking)
        print('Carrier:',carrier)
        print('Products:',products)
        print('Quantity Ordered:',quantity_ordered)
        print('Quantity Shipped:',quantity_shipped)
        print('Variance:',variance)

        if order_id :
            url = 'https://api.uphance.com/pick_tickets/'
            url = url + order_id
            
            '''pick_ticket = uphance_api_call('get',url=url)
            if pick_ticket :
                total_quantity = int(pick_ticket['pick_ticket']['total_quantity'])
                print('Total Quantity:',total_quantity)
                if total_quantity != quantity_shipped:
                    short_shipped = True
            else:
                error = True'''

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
                    if not uphance_api_call('put',url=url_tc):
                        error = True                                           
                if not error :
                    url_ship = url + '/ship'    
                    if not uphance_api_call('put',url=url_ship): #send api call to mark status as 'ship' must be done after tracking or carrier info
                        error = True
                    if not error :
                        common.send_email(0,'CD_FTP_Process_info','CD processing complete:\nStream ID:' + stream_id + '\n' +
                                                                                           'Input File: ' + f + '\n' +
                                                                                           data +
                                                                                           'URL: ' + url_tc + '\n' + url_ship,"gary@mclarenwilliams.com.au")
                        print('PC_email sent')
            else:
                common.send_email(0,'Cross Docks Message: Short Ship Response','Cross Docks file: ' + f + '\n' + \
                                                             'Pick ticket id: ' + str(order_id) + '\n' + \
                                                             'Product Bar Codes: ' + str(products) + '\n' + \
                                                             'Quantity Ordered:' + str(quantity_ordered) + '\n' + \
                                                             'Quantity Shipped: ' + str(quantity_shipped) + '\n' + \
                                                             'Variance: ' + str(variance) + '\n\n' + \
                                                             'Data in CD file: \n' + data + '\n',["richard@aemery.com","gary@mclarenwilliams.com.au"])
                                                              
                
                common.send_email(0,'CD_Short_Shipped','CD short shipped:\nStream ID:' + stream_id + '\n' +
                                                                               'Input File: ' + f + '\n' +
                                                                               data,"gary@mclarenwilliams.com.au")

        else:
            error = True
    
    
    elif stream_id == 'TP' : #Purchase order return file
        po_number = get_CD_parameter(data_lines,'TP',2)
        if po_number:
            if type(po_number) == list:
                po_number = po_number[0]

            common.send_email(0,'Cross Docks Message: Purchase Order Return File Received','CD processing manual:\nStream ID:' + stream_id + '\n' +
                                                                              'Purchase Order Number' + str(po_number) + '\n\n' +
                                                                               'Input File: ' + f + '\n' +
                                                                               data,['richard@aemery.com',"gary@mclarenwilliams.com.au"])
            print('TP email sent')
        else:
            print('Failed to get Purchase Order Number from TP file')
            common.logger('Failed to get Purchase Order Number from TP file. FileName = ' + f)
            

    
    elif stream_id == 'RJ' : #file rejected by Cross Docks
        error = True
        
    else:
        error = True
        
    if error : 
        common.send_email(0,'CD_FTP_Process_error','CD processing error (check Google Cloud logs):\nStream ID:' + stream_id + '\n' +
                                                                               'Input File: ' + f + '\n' +
                                                                               data,"gary@mclarenwilliams.com.au")
        print('Error email sent')
        return False #flag error
        
    return data

def cross_docks_poll_FTP():
    proc_start_time = datetime.datetime.now()
    common.get_CD_FTP_credentials()
    files = get_pending_FTP_files() 
    if files:
        files.sort() #sort list so that MO files are done before PC files - this helps prevent subsequent pick_ticket_update events going back to CD
        print(files)
        proc_max_files = 25
        i = 0
        proc_files = []
        
        common.uphance_initiate()
        common.dropbox_initiate()
        
        for f in files:
            print(f)
            result = process_CD_file('out/pending',f)
            if result:
                print('Processing files')
                if not download_file_DBX(result,f):
                    break #if get an error from Dropbox then break processing
                move_CD_file_FTP('out/pending','out/sent',f)
                i += 1
                proc_files.append(f)
            if i >= proc_max_files:
                break

        proc_end_time = datetime.datetime.now()
        proc_elapsed_time = proc_end_time - proc_start_time
        proc_info_str = 'CD Files Processed:\nNum Files : ' + str(i) + '\nStart Time (UTC): ' + proc_start_time.strftime("%H:%M:%S") + '\n' + \
                        'End Time (UTC): ' + proc_end_time.strftime("%H:%M:%S") + '\n' + \
                        'Elapsed Time: ' + str(proc_elapsed_time) + '\n' + \
                        'Files Processed: ' + str(proc_files)
        common.send_email(0,'CD Files Processed',proc_info_str,'gary@mclarenwilliams.com.au')
    else:
        print('No files to process')
        proc_end_time = datetime.datetime.now()
        proc_elapsed_time = proc_end_time - proc_start_time
        
        common.send_email(0,'CD Files Processed','No files processed\nElapsed Time: ' + str(proc_elapsed_time),'gary@mclarenwilliams.com.au')
    
    
@functions_framework.http
def cross_docks_poll_request(request):
    #assume all good to respond with HTTP 200 response
    #x = threading.Thread (target = uphance_process_webhook,args=(request,))
    #x.start()
    common.initialise_exception_logging()
    cross_docks_poll_FTP()
    return '200'  #need string to give HTTP 200 response

#uphance_test_webhook(CD_ff.CD_test_request)