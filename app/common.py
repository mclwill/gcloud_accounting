import sys
import smtplib
import time
import random
import traceback
import google.cloud.secretmanager as secretmanager
import google_crc32c
import dropbox
import logging
import logging.handlers
import json
import requests
from datetime import datetime

#initialise parameters
initiate_done = False
sender_pw = False 

dbx = False

uphance_headers = False
uphance_register_url = 'https://api.uphance.com/organisations/set_current_org'
uphance_org_id = 36866

cross_docks_username = "ftpemprod"
cross_docks_pw = False

logger = False


def access_secret_version(secret_id: str, version: str) -> secretmanager.AccessSecretVersionResponse:
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/227300495808/secrets/{secret_id}/versions/{version}"

    with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
        print('Access secret')
        print('Secret ID',secret_id,version)
        
    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
        print('Response',response.payload.data)
    
    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected in Google Secrets")
        logger.exception("Data corruption detected in Google Secrets")
        raise

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    #print(f"Plaintext: {payload}")

    return payload

def logging_initiate ():
    global sender_pw
    global logger

    with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
        print('Begin Initiate')
        print('Zd password',sender_pw)
        
    #if not sender_pw: #so only get pw once per session
    #    sender_pw = access_secret_version('zd_zapier_pw','1')
    
    with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
        print('After password')

    '''
    smtp_handler = logging.handlers.SMTPHandler(mailhost=('smtp.gmail.com', 587),
                                                fromaddr="zd_zapier@mclarenwilliams.com.au", 
                                                toaddrs="gary@mclarenwilliams.com.au",
                                                subject=u"Uphance Program Exception",
                                                credentials=('zd_zapier@mclarenwilliams.com.au', sender_pw),
                                                secure=())
    '''
    
    with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
        print('After SMTP handler')
        
    file_handler = logging.FileHandler('/var/log/cd-uphance/file_h.log')
    stream_handler = logging.StreamHandler()

    with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
        print('Handlers Set')
    
    #smtp_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)
    stream_handler.setLevel(logging.INFO)
    #smtp_handler.setFormatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(format)
    stream_handler.setFormatter(format)
    logger = logging.getLogger(__name__)
    #logger.addHandler(smtp_handler)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.warning('logging started')


def send_email(email_counter,message_subject,message_text,receiver_email_address):
    global sender_pw
    
    if not sender_pw: #so only get pw once per session
        sender_pw = access_secret_version('zd_zapier_pw','1')
    
    email_counter += 1
    sender_email = 'zd_zapier@mclarenwilliams.com.au'
 
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

            if type(receiver_email_address) == str:
                receiver_email_address = [receiver_email_address]

            #Defining The Message 
            message = "From: aemery_gcf <zd_zapier@mclarenwilliams.com.au>\nTo:  %s\r\n" % ",".join(receiver_email_address) + 'Subject: ' + message_subject + '\n\n' + message_text

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
                send_email(email_counter,message_subject + ' depth: ' + str(email_counter),message_text,receiver_email_address)
                email_counter -= 1
                return True
            logger.exception('send mail SMTP error',exc_info = True)
            email_counter -= 1
            return False

        except Exception as ex:
            tb = traceback.format_exc()
            print('Other email exception')
            print('Exception info:',e)
            print(tb)
            raise


    else: #too many active emails - wait after random time period to send again
        time.sleep(random.random()*5)
        send_email(email_counter,message_subject + ' depth 5 or more: ' + str(email_counter),message_text,receiver_email_address)
        email_counter -= 1
        return False

def dropbox_initiate():
    global dbx
    if not dbx:
        aemery_dbx_app_key = access_secret_version('dbx_app_key','1')
        aemery_dbx_app_secret = access_secret_version('dbx_app_secret','1')
        aemery_dbx_refresh_token = access_secret_version('dbx_refresh_token','1')
        dbx = dropbox.Dropbox(app_key=aemery_dbx_app_key,app_secret=aemery_dbx_app_secret,oauth2_refresh_token=aemery_dbx_refresh_token)
        try:
            check = dbx.check_app(query = 'Test Me')
            #print(check,dir(check))
            if check.result == 'Test Me':
                return True
            else:
                dbx = False
                tb = traceback.format_exc()
                send_email(0,'Error Initialising Dropbox','Check result is: ' + str(check.result) + '\nTraceback:\n' + tb, "gary@mclarenwilliams.com.au")
                return False
        except Exception as ex:
            print('Exception:',ex)
            tb = traceback.format_exc()
            send_email(0,'Error Initialising Dropbox','Exception: ' + str(ex) + '\nTraceback:\n' + tb, "gary@mclarenwilliams.com.au")
            dbx = False
            return False

#dropbox_initiate()
#print('dbx:',dbx)

def uphance_initiate():
    global uphance_headers
    global logger

    if not uphance_headers :
        uphance_secret = json.loads(access_secret_version('uphance_access_token','1'))
        #print(uphance_secret)
        uphance_expires = datetime.utcfromtimestamp(uphance_secret['created_at'] + uphance_secret['expires_in'])
        uphance_headers = {'Authorization': 'Bearer '+ uphance_secret['access_token'],'Content-Type': 'application/json'}
        uphance_register = {'organizationId': uphance_org_id}
        try:
            response = requests.post(uphance_register_url,json = uphance_register,headers = uphance_headers)
    
            if response.status_code == 201:
                print('Uphance initiated')
                print(response.json())
                print('Uphance token expires on: ',uphance_expires.strftime('%Y-%m-%d'))
                td = uphance_expires - datetime.now()
                if td.days < 30 :
                    send_email(0,'Uphance access token expiry','Uphance token will expire in ' + str(td.days) + ' days\nNeed to get new token and store in Google Secret Manager','gary@mclarenwilliams.com.au')
                #else:
                #    send_email(0,'Uphance access token expiry','Uphance token will expire in ' + str(td.days) + ' days\nNeed to get new token and store in Google Secret Manager','gary@mclarenwilliams.com.au')
                return True
            else:
                print(response.status_code)
                logger.exception('Problem initiating Uphance: Response Status Code = ' + str(response.status_code))
                raise
        except Exception as ex:
            logger.exception('Error initiating Uphance\n' + str(ex))
            raise
    else:
        print('Uphance already initiated')
    return True

def get_CD_FTP_credentials():
    global cross_docks_pw, cross_docks_username
    
    if not cross_docks_pw:
        cross_docks_pw = access_secret_version('cross_docks_pw','1')

    return cross_docks_username, cross_docks_pw

def check_initiate_done():
    global initiate_done

    if not initiate_done:
        logging_initiate()
        initiate_done = True


#initialise_exception_logging()
#uphance_initiate()

