import os
import io
import smtplib
import time
import random
import traceback
import dropbox
import logging
import logging.handlers
import json
import requests
from datetime import datetime
from contextlib import closing
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import base64
from google.oauth2 import service_account
from googleapiclient.discovery import build
from requests import HTTPError


import FlaskApp.app.secrets as secrets
from FlaskApp.app import app

#used for setting testing on and off - False for testing purposes True for production

if ('LOCAL' in app.config) and app.config['LOCAL']:
    running_local = True
    FTP_active = False
    Dropbox_active = False
    Uphance_active = False
    working_dir = ''
else:
    running_local = False
    FTP_active = True
    Dropbox_active = True
    Uphance_active = True
    working_dir = '/var/www/FlaskApp'

SERVICE_ACCOUNT_FILE = os.path.join(working_dir,'FlaskApp/app','service_key.json')
DELEGATED_USER = 'zd_zapier@mclarenwilliams.com.au'
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def access_secret_version(secret_id: str, customer: str, parameter: str):
    attribute = getattr(secrets,secret_id)
    #logger.debug('Secrets.py data: ' + str(attribute))
    if customer :
        secret = attribute[customer][parameter]
    else:
        secret = attribute[parameter]
    return secret

def json_dump(file,variable):
    if ('LOCAL' in app.config) and app.config['LOCAL']:
        file_prefix = './FlaskApp/app/'
    else:
        file_prefix = '/var/www/FlaskApp/FlaskApp/app/'
    try:
        with open (file_prefix + file,"w") as outfile:
            json.dump(variable,outfile)
        return True
    except :
        raise Exception('Storing of variable to json file' + file + 'failed')
        return False


def json_load(file):
    if ('LOCAL' in app.config) and app.config['LOCAL']:
        file_prefix = './FlaskApp/app/'
    else:
        file_prefix = '/var/www/FlaskApp/FlaskApp/app/'
    try:
        with open(file_prefix + file) as infile:
            return json.load(infile)
    except FileNotFoundError as fnf_error:
        logger.warning(str(fnf_error))
        return False

class GmailLoggingHandler(logging.Handler): #from ChatGPT 30/11/24
    def __init__(self, service_account_file, delegated_user, sender, recipient):
        super().__init__()
        self.service_account_file = service_account_file
        self.delegated_user = delegated_user
        self.sender = sender
        self.recipient = recipient
        self.service = self.authenticate_gmail_api()

    def authenticate_gmail_api(self):
        SCOPES = ['https://www.googleapis.com/auth/gmail.send']
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=SCOPES)
        delegated_credentials = credentials.with_subject(self.delegated_user)
        service = build('gmail', 'v1', credentials=delegated_credentials)
        return service

    def create_message(self, subject, message_text):
        message = MIMEText(message_text)
        message['to'] = self.recipient
        message['from'] = self.sender
        message['subject'] = subject
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return {'raw': raw}

    def send_message(self, message):
        try:
            sent_message = (self.service.users().messages().send(userId='me', body=message).execute())
            print('Message Id: %s' % sent_message['id'])
        except Exception as error:
            print(f'An error occurred: {error}')

    def emit(self, record):
        log_entry = self.format(record)
        subject = f"UPHANCE-CROSSDOCKS-LOGGING: {record.levelname}"
        message = self.create_message(subject, log_entry)
        self.send_message(message)

def logging_initiate ():
    global logger,server

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    new_relic_handler = logging.StreamHandler()
    #file_handler = logging.FileHandler('/var/log/cd-uphance/file_h.log')
    stream_handler = logging.StreamHandler()
    
    #file_handler.setLevel(logging.DEBUG)
    stream_handler.setLevel(logging.DEBUG)

    format = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
    #file_handler.setFormatter(format)
    stream_handler.setFormatter(format)
    
    #logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    

    logger.debug('Attempting to start SMTP logging')
    sender_pw = access_secret_version('global_parameters',None,'email_pw')
    #logger.debug('Sender PW: ' + sender_pw)
    smtp_handler = logging.handlers.SMTPHandler(mailhost=('smtp.gmail.com', 587),
                                                fromaddr=access_secret_version('global_parameters',None,'from_email'),
                                                toaddrs=access_secret_version('global_parameters',None,'emails'),
                                                subject=u"Cross Docks Uphance Google VM Logging: " + server,
                                                credentials=(access_secret_version('global_parameters',None,'from_email'), sender_pw),
                                                secure=())
    gmail_handler = GmailLoggingHandler(SERVICE_ACCOUNT_FILE, DELEGATED_USER, access_secret_version('global_parameters',None,'from_email'), ','.join(access_secret_version('global_parameters',None,'emails')))

    smtp_handler.setLevel(logging.INFO)
    gmail_handler.setLevel(logging.INFO)
    smtp_handler.setFormatter(format)
    gmail_handler.setFormatter(format)
    logger.addHandler(gmail_handler)
    #logger.addHandler(smtp_handler)

    logger.debug('Cross Docks - Uphance API: Logging started for File, Stream and SMTP logging')

def send_email(email_counter,message_subject,message_text,dest_email,**kwargs):
    global SERVICE_ACCOUNT_FILE, DELEGATED_USER
    #not using email_count - kept to keep backward compatibility with old send_mail procedure
    attachments = kwargs.pop('attachments',None)
    reply_to = kwargs.pop('reply_to',None)
    cc = kwargs.pop('cc',None)
    bcc = kwargs.pop('bcc',None)
    customer = kwargs.pop('customer',None)

    #CLIENT_ID = access_secret_version('global_parameters',None,'google_client_id')
    #CLIENT_SECRET = access_secret_version('global_parameters',None,'google_client_secret')
    #SERVICE_ACCOUNT_FILE = os.path.join(working_dir,'FlaskApp/app','service_key.json')
    #DELEGATED_USER = 'support@zoedaniel.com.au'
    SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    delegated_creds = creds.with_subject(DELEGATED_USER)

    ##flow = InstalledAppFlow.from_client_secrets_file(
    #    CLIENT_SECRET_FILE, scopes=SCOPES)

    #creds = flow.run_local_server(port=0)
    service = build('gmail', 'v1', credentials=delegated_creds)

    sender_email = access_secret_version('global_parameters',None,'from_email')

    if type(dest_email) == str:
                receiver_email_address = [dest_email]
    elif type(dest_email) == list:
        receiver_email_address = []
        for text in dest_email:
            if text == 'global':
                for e in access_secret_version('global_parameters',None,'emails'):
                    receiver_email_address.append(e)
            elif text == 'customer':
                for e in access_secret_version('customer_parameters',customer,'emails'):
                    receiver_email_address.append(e)
            elif type(text) == dict:
                if customer in text.keys():
                    for e in text[customer]:
                        receiver_email_address.append(e)
            else:
                receiver_email_address.append(text)
    else:
        raise Exception('Error sending email: send email destination address in the wrong fomat. dest_email: ' + str(dest_email))
        smtp.quit()
        return False

    if type(cc) == str:
        cc = [cc] #convert to array
    if type(bcc) == str:
        bcc = [bcc]

    #new email code for attachments from: https://stackoverflow.com/questions/3362600/how-to-send-email-attachments

    logger.debug('Destination email address(es):' + str(receiver_email_address))

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = COMMASPACE.join(receiver_email_address)
    msg['Date'] = formatdate(localtime=True)
    if customer :
        msg['Subject'] = customer + ' : ' + message_subject
    else:
        msg['Subject'] = message_subject
    if reply_to:
        msg.add_header('reply-to',reply_to)
    if cc:
        msg['Cc'] = COMMASPACE.join(cc)
        logger.debug(str(msg['Cc']))
    
    #Don't put bcc in msg as then it won't be blind
    #if bcc:
    #    msg['Bcc'] = bcc
    
    msg.attach(MIMEText(message_text,'plain'))

    for f in attachments or [] :
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    create_message = {'raw': base64.urlsafe_b64encode(msg.as_bytes()).decode()}
    try:
        message = (service.users().messages().send(userId="me", body=create_message).execute())
        logger.debug(F'sent message to {message} Message Id: {message["id"]}')
        return True
    except HTTPError as error:
        logger.error(F'Send email API error occurred: {error}')
        message = None
        return False

def send_email_old(email_counter,message_subject,message_text,dest_email,**kwargs):
    customer = kwargs.pop('customer',None)

    email_counter += 1
    if customer:
        sender_email = access_secret_version('customer_parameters',customer,'reporting_email')
        sender_pw = access_secret_version('customer_parameters',customer,'reporting_email_pw')
    else:
        sender_email = access_secret_version('global_parameters',None,'from_email')
        sender_pw = access_secret_version('global_parameters',None,'email_pw')

    if email_counter < 0:
        logger.exception('Email counter below zero: ' + str(email_counter) + ' ' + message_subject + ' ' + message_text)
        return False
    if email_counter <= 5:
        
        try: 
            #Create your SMTP session 
            smtp = smtplib.SMTP('smtp.gmail.com', 587) 

           #Use TLS to add security 
            smtp.starttls() 

            #User Authentication 

            #print('sender password',sender_pw)
            smtp.login(sender_email,sender_pw)

            #dest_email can be either one email address, list of email addresses or a list with 'global' and/or 'customer' to pick up the configured data in secrets.py

            if type(dest_email) == str:
                receiver_email_address = [dest_email]
            elif type(dest_email) == list:
                receiver_email_address = []
                for text in dest_email:
                    if text == 'global' or (not customer) :
                        for e in access_secret_version('global_parameters',None,'emails'):
                            receiver_email_address.append(e)
                    elif text == 'customer':
                        for e in access_secret_version('customer_parameters',customer,'emails'):
                            receiver_email_address.append(e)
                    else:
                        receiver_email_address.append(text)
            else:
                raise Exception('Error sending email: send email destination address in the wrong fomat. dest_email: ' + str(dest_email))
                smtp.quit()
                return False

            smtp_from = 'From: ' + access_secret_version('global_parameters',None,'from_name') + '<' + access_secret_version('global_parameters',None,'from_email') + '>\n'

            #Defining The Message 
            if not customer:
                customer = 'No customer'
            message = smtp_from + "To:  %s\r\n" % ",".join(receiver_email_address) + 'Subject: ' + customer + ' : Uphance Cross Docks message ' + message_subject + '\n\n' + message_text

            #Sending the Email
            smtp.sendmail(sender_email, receiver_email_address,message.encode('utf-8')) 

            #Terminating the session 
            smtp.quit() 
            return True

        except smtplib.SMTPResponseException as ex:
            error_count = 0
            error_code = ex.smtp_code
            error_message = ex.smtp_error
            time.sleep(random.random()*5)
            #logger.exception('send mail SMTP error',exc_info = True)
            if error_code == 421 : #try again after random time interval up to 5 seconds
                time.sleep(random.random()*5)
                send_email(email_counter,message_subject + ' depth: ' + str(email_counter),message_text,receiver_email_address,customer=customer)
                email_counter -= 1
                return True
            tb = traceback.format_exc()
            logger.warning('Send mail SMTP error - no retry performed' + '/nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
            email_counter -= 1
            return False

        except Exception as ex:
            tb = traceback.format_exc()
            logger.warning('Other email error - no retry performed' + '/nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
            email_counter -= 1
            return False


    else: #too many active emails - wait after random time period to send again
        time.sleep(random.random()*5)
        send_email(customer,email_counter,message_subject + ' depth 5 or more: ' + str(email_counter),message_text,receiver_email_address)
        email_counter -= 1
        return False

def getLocalFiles(folder,**kwargs):
    customer = kwargs.pop('customer','No customer')
    #error = kwargs.pop('error',None)
    #request_dict = kwargs.pop('request_dict',None)
    
    
    localfiles = []
    try:
        for (root,dirs,files) in os.walk(folder,topdown=True):
            for f in files:
                with open(os.path.join(folder,f),'r') as text_file:
                    filedata = text_file.read()
                t = os.path.getmtime(os.path.join(folder,f))
                file_item = {'file_name':f,'file_data':filedata,'mod_time':datetime.fromtimestamp(t)}
                localfiles.append(file_item)

        return True, localfiles
    
    except Exception as ex:
        logger.warning('Logging Warning Error for : ' + customer + '\nUphance_webhook_error : Local File Reading Error \nFile Names: ' + str(local_files) + '\nError:' + str(ex))
        return False, None


def storeLocalFile(folder,file_name,file_data,**kwargs) :
    customer = kwargs.pop('customer','No customer')
    error = kwargs.pop('error',None)
    request_dict = kwargs.pop('request_dict',None)

    try:
        with open(os.path.join(folder,file_name),'w') as text_file:
            text_file.write(file_data)
        return True 
    except Exception as ex:
        logger.warning('Logging Warning Error for : ' + customer + '\nUphance_webhook_error : Local File Save Error \nFile Name: ' + file_name + '\nError Info: ' + str(error) + '\nError:' + str(ex) + '\nOutput file:\n' + file_data + '\nInput Request:\n' + str(request_dict))
        return False

def dropbox_initiate():
    global dbx
    if not dbx:
        dbx_app_key = access_secret_version('global_parameters',None,'dbx_app_key')
        dbx_app_secret = access_secret_version('global_parameters',None,'dbx_app_secret')
        dbx_refresh_token = access_secret_version('global_parameters',None,'dbx_refresh_token')
        dbx = dropbox.Dropbox(app_key=dbx_app_key,app_secret=dbx_app_secret,oauth2_refresh_token=dbx_refresh_token)
        try:
            check = dbx.check_app(query = 'Test Me')
            #print(check,dir(check))
            if check.result == 'Test Me':
                return True
            else:
                dbx = False
                tb = traceback.format_exc()
                send_email(0,'Error Initialising Dropbox','Check result is: ' + str(check.result) + '\nTraceback:\n' + tb, ['global'])
                return False
        except Exception as ex:
            print('Exception:',ex)
            tb = traceback.format_exc()
            send_email(0,'Error Initialising Dropbox','Exception: ' + str(ex) + '\nTraceback:\n' + tb, ['global'])
            dbx = False
            return False

def uphance_check_token_status(customer):
    global uphance_access_token

    uphance_token_refresh = False

    if not uphance_access_token : #uphance_access_token not loaded
        uphance_access_token = json_load('uphance_access_tokens.json') #try to load from json file
        if not uphance_access_token: #if unsuccessful then create refresh token
            uphance_token_refresh = True
    
    if not uphance_token_refresh:
        if customer in uphance_access_token: #if uphance token exists for customer check if near expiry
            uphance_expires = datetime.utcfromtimestamp(uphance_access_token[customer]['created_at'] + uphance_access_token[customer]['expires_in'])
            td = uphance_expires - datetime.now()
            if td.days < 30 :
                logger.info('Uphance access token expiry - Uphance token will expire in ' + str(td.days) + ' days' + ' for ' + customer + '\nGetting new access token')
                uphance_token_refresh = True
        else:
            uphance_token_refresh = True

    if uphance_token_refresh:
        uphance_token_url = 'https://api.uphance.com/oauth/token'
        uphance_headers = {'Content-Type': 'application/json'}
        uphance_login = access_secret_version('customer_parameters',customer,'uphance_login')
        uphance_get_token = {'email': uphance_login['username'],
                         'password' : uphance_login['password'],
                         'grant_type' : 'password'}
        try: 
            response = requests.post(uphance_token_url,json = uphance_get_token,headers = uphance_headers)
            #logger.debug('token after request' + str(response.json()))
            if response.status_code == 200:
                logger.info('New token fetched for ' + customer)
                if not uphance_access_token : #dict doesn't exist so need to create it
                    uphance_access_token = {}
                uphance_access_token[customer] = response.json()
                json_dump('uphance_access_tokens.json',uphance_access_token)
                logger.info('Uphance access token renewed for ' + customer)
            else:
                logger.warning('Problem getting new access token for Uphance for '+ customer + ' : Response Status Code = ' + str(response.status_code))
                return False
        except Exception as ex:
            logger.warning('Exception getting new access token for Uphance for ' + customer + '\n' + str(ex))
            return False

    return True

def uphance_initiate(customer:str, **kwargs):
    force_initiate = kwargs.pop('force_initiate',None)
    global uphance_headers, uphance_access_token
    global logger, server

    if uphance_check_token_status(customer):

        if (not uphance_headers[customer]) or force_initiate :
            uphance_expires = datetime.utcfromtimestamp(uphance_access_token[customer]['created_at'] + uphance_access_token[customer]['expires_in'])
            uphance_headers[customer] = {'Authorization': 'Bearer '+ uphance_access_token[customer]['access_token'],'Content-Type': 'application/json'}
            uphance_register = {'organizationId': uphance_org_id[customer]}
            try:
                response = requests.post(uphance_register_url,json = uphance_register,headers = uphance_headers[customer])
        
                if response.status_code == 201:
                    if server == 'Production':
                        logger.info('\nLogger Info for ' + customer + ' Uphance initiated and Uphance token expires on: '+ uphance_expires.strftime('%Y-%m-%d'))
                    else:
                        logger.debug('\nLogger Info for ' + customer + ' Uphance initiated and Uphance token expires on: '+ uphance_expires.strftime('%Y-%m-%d'))
                    logger.debug(response.json())
                    return True
                else:
                    logger.warning('Problem initiating Uphance for '+ customer + ' : Response Status Code = ' + str(response.status_code))
                    return False
            except Exception as ex:
                logger.warning('Exception occurred while trying to initiate Uphance for ' + customer + '\n' + str(ex))
                tb = traceback.format_exc()
                logger.warning(tb)
                return False
        else:
            logger.info('Uphance already initiated')
        return True

    else:
        return False

def get_CD_FTP_credentials(customer:str):
    global cross_docks_info

    if (not cross_docks_info[customer]):
        cross_docks_info[customer] = {'username':access_secret_version('customer_parameters',customer,'cross_docks_FTP_username'),
                                      'password':access_secret_version('customer_parameters',customer,'cross_docks_FTP_pw')}

    return cross_docks_info[customer]

def check_logging_initiate():
    global initiate_logging_done

    if not initiate_logging_done:
        logging_initiate()
        logger.debug('Initiate logging done')
        initiate_logging_done = True

def check_uphance_initiate():
    global customers

    for c in customers:
        uphance_running[c] = uphance_initiate(c)

def uphance_api_call(customer,api_type,**kwargs):
    url = kwargs.pop('url',None)
    json = kwargs.pop('json',None)
    _override = kwargs.pop('override',None)
    
    if Uphance_active or _override or api_type == 'get':


        return_error = False
        
        if api_type == 'post':
            response = requests.post(url,json = json,headers = uphance_headers[customer])
            logger.debug('Post ' + url)
        elif api_type == 'put':
            response = requests.put(url,headers = uphance_headers[customer])
            logger.debug('Put ' + url)
        elif api_type == 'get' :
            response = requests.get(url,headers = uphance_headers[customer])
            logger.debug('Get ' + url)
        else:
            logger.warning('Error in api_type: ' + api_type)
            return_error = 'Error in api_type'
            return return_error, 'NULL'

        if response.status_code == 200:
            logger.debug('Uphance ' + api_type + ' successful for ' + customer)
            logger.debug(response.json())
            return return_error, response.json()  #this should be a False
        else:
            logger.warning('Uphance ' + api_type + ' error for ' + customer + '\nURL: ' + url + '\nResponse Status Code: ' + str(response.status_code))
            return str(response.status_code), 'NULL'

    else:
        #this coding used for testing only so that Uphance is not updated
        logger.info('Dummy API uphance call for ' + customer + '\n' + api_type  + '\n' + str(url) + '\n' + str(json))
        return False, 'Testing Call to uphance_api_call'
        #end of testing code


def read_dropbox_bytestream(customer,file_path):
    global dbx
    
    #see https://stackoverflow.com/questions/53697160/how-do-i-read-an-excel-file-directly-from-dropboxs-api-using-pandas-read-excel
    
    try: 
        _, res = dbx.files_download(file_path)
        with closing(res) as result:
            byte_data = result.content
            logger.debug('Dropbox Read done successfully:' + file_path)
            return io.BytesIO(byte_data)

    except Exception as ex:
        tb = traceback.format_exc()
        logger.warning('Logging Warning Error for :' + customer + ' Exception in read_dropbox\nFile Path: ' + file_path + '\nDropbox Error:' + str(ex) + '\nTraceback:\n' + str(tb))
        logger.debug('Dropbox Read Error')
        return False


def store_dropbox(customer,file_data,file_path,retry=False,**kwargs):
    
    global dbx

    _override = kwargs.pop('override',None)
    #below exception handling implemented 2024-08-09 to cope with intermittent dropbox errors
    if Dropbox_active or _override:
        try:
            #if not retry:
            #    raise Exception('Simulate Dropbox error')
            with io.BytesIO(file_data.encode()) as stream:
                stream.seek(0)
                dbx.files_upload(stream.read(), file_path, mode=dropbox.files.WriteMode.overwrite)
            logger.debug('Dropbox Transferred Successfully : ' + file_path)
            #check for any failed dropbox transfers
            if not retry:
                dbx_folder = access_secret_version('customer_parameters',customer,'dbx_folder')
                for (root,dirs,files) in os.walk(os.path.join('/home/gary/dropbox',customer),topdown=True):
                    for d in dirs:
                        queuedFiles = getLocalFiles(d,customer=customer)
                        if queuedFiles[0] : #only process files if no errors on getting Local files
                            for file_item in queuedFiles[1]:
                                if store_dropbox(customer,file_item['file_data'],os.path.join(dbx_folder,os.path.basename(os.path.normpath(d)),file_item['file_name']),True): #flag this is a retry to avoid another saving on error
                                    os.remove(os.path.join(d,file_item['file_name'])) #remove file if dropbox store is successful successful
                                    logger.info('Logger Info for ' + customer + '\nLocal file successully transferred to dropbox and removed locally\nFile: ' + file_item['file_name'])
            return True 

        except Exception as ex:
            tb = traceback.format_exc()
            logger.warning('Logging Warning Error for :' + customer + ' Exception in store_dropbox\nFile Path: ' + file_path + '\nDropbox Error:' + str(ex) +  '\nTraceback:\n' + str(tb))
            logger.debug('Dropbox Transfer Error - will store locally and retry : ' + file_path)
            if not retry:
                file_loc = os.path.basename(os.path.normpath(file_path))
                if file_loc == 'sent' or file_loc == 'received': #filter out only regular CD file saving errors
                    storeLocalFile(os.path.join('home/gary/dropbox',customer,file_loc),file_name,file_data,customer=customer)  #store file locally
            return False
    else:
        logger.info('File not sent to Dropbox as inactive for testing\nFile Path: ' + file_path + '\nFile Data:\n' + file_data )
        return True 

def get_users():
    global customers

    users = {}
    for c in customers:
        user_c = access_secret_version('customer_parameters',c,'dashboard_auth')
        if user_c:
            for k,v in user_c.items():
                users[k] = v
    return users

def get_dropbox_file_info(customer,file_path,**kwargs):
    try:
        from_date = kwargs.pop('from_date',None)
        file_spec = kwargs.pop('file_spec',None)  #list of strings to be contained in file name
        files_list = []
        files_info = dbx.files_list_folder(file_path)
        more_info = True
        cursor = None
        while more_info:
            if not cursor :
                files_info = dbx.files_list_folder(file_path)
            else:
                files_info = dbx.files_list_folder_continue(cursor)
            for file in files_info.entries:
                if isinstance(file, dropbox.files.FileMetadata):
                    metadata = {
                        'name': file.name,
                        'path_display': file.path_display,
                        'client_modified': file.client_modified,
                        'server_modified': file.server_modified
                    }
                    files_list.append(metadata)
            more_info = files_info.has_more
            cursor = files_info.cursor
        
        if from_date:
            if type(from_date) is not datetime:
                from_date = datetime.strptime(from_date,'%d/%m/%Y')
            filtered_list = []
            for files in files_list:
                if files['client_modified'] >= from_date:
                    if not file_spec : 
                        filtered_list.append(files)
                    else:
                        for f in file_spec:
                            if f in files['name']:
                                filtered_list.append(files)
            return filtered_list
        else:
            return files_list
    except Exception as ex:
        tb = traceback.format_exc()
        logger.warning('Logging Warning Error for :' + customer + ' Exception in get_dropbox_file_info\nFile Path: ' + file_path + '\nDropbox Error:' + str(ex) + '\nTraceback:\n' + str(tb))
        logger.debug('Dropbox Read Error')
        return False


#initialise parameters

#get information on google cloud environment
if not (('LOCAL' in app.config) and app.config['LOCAL']):
    metadata_server = "http://metadata/computeMetadata/v1/instance/"
    metadata_flavor = {'Metadata-Flavor' : 'Google'}
    gce_id = requests.get(metadata_server + 'id', headers = metadata_flavor).text
    gce_name = requests.get(metadata_server + 'hostname', headers = metadata_flavor).text
    gce_machine_type = requests.get(metadata_server + 'machine-type', headers = metadata_flavor).text

    #if 'test' in gce_name:
    #    server = "Test"
    #else:
    server = "Production"
else:
    server = 'Test'

#initiate logging
initiate_logging_done = False
logger = False
check_logging_initiate()

#initiate dropbox
dbx = False
dropbox_initiate()

#initiate uphance 
customers = access_secret_version('global_parameters',None,'customers')

uphance_headers = {}
uphance_running = {}
uphance_access_token = False
data_store = {}

for c in customers:
    uphance_headers[c] = False
    uphance_running[c] = False
    data_store[c] = access_secret_version('customer_parameters',c,'data_store_folder')

uphance_register_url = access_secret_version('global_parameters',None,'uphance_register_url')
uphance_org_id = access_secret_version('global_parameters',None,'uphance_org_id')

check_uphance_initiate()

#initiate cross_docks
cross_docks_info = {}
for c in customers:
    cross_docks_info[c] = False
    get_CD_FTP_credentials(c)











