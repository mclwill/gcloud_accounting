from FlaskApp.app import app
#import time
import sys
import threading
from flask import request, jsonify
import FlaskApp.app.common as common
import FlaskApp.app.uphance_webhook_info as uphance_webhook
import FlaskApp.app.cross_docks_polling as cross_docks_polling
import FlaskApp.app.data_store as data_store


@app.route("/")
def homepage():
    common.logger.debug('root of domain reached displaying : Nothing to see here')
    return "Nothing to see here"

@app.route('/test',methods=['POST','GET'])
def test():
    #args = None
    content = request.get_json(silent=True)
    if content:
        common.send_email(0,'Test Message',str(content),'gary@mclarenwilliams.com.au')
    else:
        common.send_email(0,'Test Message','No content','gary@mclarenwilliams.com.au')

    return 'Test Processed - check email'

@app.route('/uphance',methods=['POST','GET'])
def uphance():
    #args = None
    if common.uphance_initiate('aemery'):
        #common.send_email(0,'Uphance initiated successfully',str(content),'gary@mclarenwilliams.com.au')
        return 'Uphance initiated successfully'
    else:
        #common.send_email(0,'Uphance not initiated','No content','gary@mclarenwilliams.com.au')
        return 'Uphance not initiated'


@app.route('/aemery',methods=['POST'])
def process_aemery_webhook():
    content = request.get_json(silent=True)
    if content:
        status_code = uphance_webhook.uphance_prod_webhook('aemery',content)
        return 'Processed', status_code
    else :
        return 'amery - No content'

@app.route('/aemery_cross-docks-polling',methods=['POST'])
def process_aemery_cross_docks_polling():

    status_code = cross_docks_polling.cross_docks_poll_request('aemery')
    
    return 'Processed', status_code

@app.route('/two-ts',methods=['POST'])
def process_two_ts_webhook():
    content = request.get_json(silent=True)
    if content:
        status_code = uphance_webhook.uphance_prod_webhook('two-ts',content)
        return 'Processed', status_code
    else :
        return 'two-ts No content'

@app.route('/two-ts_cross-docks-polling',methods=['POST'])
def process_two_ts_cross_docks_polling():

    status_code = cross_docks_polling.cross_docks_poll_request('two-ts')
    
    return 'Processed', status_code


@app.route('/aemery_get_product_data',methods=['POST'])
def process_aemery_get_data_store_info():
    #use threading so that no timeout occurs on POST
    x = threading.Thread(target = data_store.get_data_store_info, args=('aemery',))
    x.start()
    return 'Processed using thread'