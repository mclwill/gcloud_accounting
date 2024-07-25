import sys
import os
import smtplib
import time
import random
import traceback
import google.cloud.secretmanager as secretmanager
from google.oauth2 import service_account
import google_crc32c
import dropbox
import logging
import logging.handlers
import json
import requests
from datetime import datetime
#from newrelic.agent import NewRelicContextFormatter
import FlaskApp.app.secrets as secrets


'''def access_secret_version(secret_id: str, version: str) -> secretmanager.AccessSecretVersionResponse:
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """
    
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/227300495808/secrets/{secret_id}/versions/{version}"

    logger.debug('Access Secret with name: ' + name)
        
    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    logger.debug('Response payload data: ' + response.payload.data)
    
    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        logger.error("Data corruption detected in Google Secrets\nResponse.payload.data : " + response.payload.data)
        logger.exception("Data corruption detected in Google Secrets")
        raise

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    #print(f"Plaintext: {payload}")

    return payload'''

def access_secret_version(secret_id: str, customer: str, parameter: str):
    attribute = getattr(secrets,secret_id)
    #logger.debug('Secrets.py data: ' + str(attribute))
    if customer :
        secret = attribute[customer][parameter]
    else:
        secret = attribute[parameter]
    return secret

def json_dump(file,variable):
    try:
        with open ('/var/www/FlaskApp/FlaskApp/app/' + file,"w") as outfile:
            json.dump(variable,outfile)
        return True
    except :
        raise Exception('Storing of variable to json file' + file + 'failed')
        return False


def json_load(file):
    try:
        with open('/var/www/FlaskApp/FlaskApp/app/' + file) as infile:
            return json.load(infile)
    except FileNotFoundError as fnf_error:
        logger.warning(str(fnf_error))
        return False

def logging_initiate ():
    global logger

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    new_relic_handler = logging.StreamHandler()
    file_handler = logging.FileHandler('/var/log/cd-uphance/file_h.log')
    stream_handler = logging.StreamHandler()
    
    file_handler.setLevel(logging.DEBUG)
    stream_handler.setLevel(logging.DEBUG)

    format = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
    file_handler.setFormatter(format)
    stream_handler.setFormatter(format)
    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    

    logger.debug('Attempting to start SMTP logging')
    sender_pw = access_secret_version('global_parameters',None,'email_pw')
    #logger.debug('Sender PW: ' + sender_pw)
    smtp_handler = logging.handlers.SMTPHandler(mailhost=('smtp.gmail.com', 587),
                                                fromaddr=access_secret_version('global_parameters',None,'from_email'),
                                                toaddrs=access_secret_version('global_parameters',None,'emails'),
                                                subject=u"Cross Docks Uphance Google VM Logging",
                                                credentials=(access_secret_version('global_parameters',None,'from_email'), sender_pw),
                                                secure=())
    smtp_handler.setLevel(logging.INFO)
    smtp_handler.setFormatter(format)
    logger.addHandler(smtp_handler)

    logger.debug('Cross Docks - Uphance API: Logging started for File, Stream and SMTP logging')


def send_email(customer,email_counter,message_subject,message_text,dest_email):

    email_counter += 1
    sender_email = access_secret_version('customer_parameters',customer,'reporting_email')
    sender_pw = access_secret_version('customer_parameters',customer,'reporting_email_pw')
 
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
                    if text == 'global':
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
                send_email(customer,email_counter,message_subject + ' depth: ' + str(email_counter),message_text,receiver_email_address)
                email_counter -= 1
                return True
            logger.exception('send mail SMTP error',exc_info = True)
            email_counter -= 1
            return False

        except Exception as ex:
            tb = traceback.format_exc()
            logger.error('Log Error: Other email exception')
            logger.error('Log Error: Exception info:'  + str(ex))
            logger.error(str(tb))
            return False


    else: #too many active emails - wait after random time period to send again
        time.sleep(random.random()*5)
        send_email(customer,email_counter,message_subject + ' depth 5 or more: ' + str(email_counter),message_text,receiver_email_address)
        email_counter -= 1
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

#dropbox_initiate()
#print('dbx:',dbx)

def uphance_check_token_status(customer):
    global uphance_access_token

    uphance_token_refresh = False

    if not uphance_access_token : #uphance_access_token not loaded
        uphance_access_token = json_load('uphance_access_tokens.json') #try to load from json file
        if not uphance_access_token: #if unsuccessful then create refresh token
            uphance_token_refresh = True
        else:
            if uphance_access_token[customer]: #if uphance token exists for customer check if near expiry
                uphance_expires = datetime.utcfromtimestamp(uphance_access_token[customer]['created_at'] + uphance_access_token[customer]['expires_in'])
                td = uphance_expires - datetime.now()
                if td.days < 30 :
                    logger.info('Uphance access token expiry','Uphance token will expire in ' + str(td.days) + ' days' + ' for ' + customer + '\nGetting new access token')
                    uphance_token_refresh = True
            else:
                uphance_token_refresh = True
    else:
        if not customer in uphance_access_token :
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
    global logger

    if uphance_check_token_status(customer):

        if (not uphance_headers[customer]) or force_initiate :
            uphance_expires = datetime.utcfromtimestamp(uphance_access_token[customer]['created_at'] + uphance_access_token[customer]['expires_in'])
            uphance_headers[customer] = {'Authorization': 'Bearer '+ uphance_access_token[customer]['access_token'],'Content-Type': 'application/json'}
            uphance_register = {'organizationId': uphance_org_id[customer]}
            try:
                response = requests.post(uphance_register_url,json = uphance_register,headers = uphance_headers[customer])
        
                if response.status_code == 201:
                    logger.info('\nLogger Info for ' + customer + '\nUphance initiated for ' + customer + '\nUphance token expires on: '+ uphance_expires.strftime('%Y-%m-%d'))
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

#initialise parameters

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

for c in customers:
    uphance_headers[c] = False
    uphance_running[c] = False

uphance_register_url = access_secret_version('global_parameters',None,'uphance_register_url')
uphance_org_id = access_secret_version('global_parameters',None,'uphance_org_id')

check_uphance_initiate()

#initiate cross_docks
cross_docks_info = {}
for c in customers:
    cross_docks_info[c] = False
    get_CD_FTP_credentials(c)











