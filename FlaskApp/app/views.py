from . import app
#import time
import sys
import threading
from flask import request, jsonify
from . import common
import urllib.parse


@app.route("/")
def homepage():
    common.logger.debug('root of domain reached displaying : Nothing to see here')
    return "Nothing to see here"

@app.route('/test',methods=['POST','GET'])
def test():
    #args = None
    content = request.get_json(silent=True)
    #common.logger.info(str(request.url))
    if content:
        common.send_email(0,'Test Message',str(content),'gary@mclarenwilliams.com.au')
    else:
        common.send_email(0,'Test Message','No content','gary@mclarenwilliams.com.au')

    return 'Test Processed - check email'

