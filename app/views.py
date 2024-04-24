from FlaskApp.app import app
import time
import sys
from flask import request, jsonify
import FlaskApp.app.common as common


@app.route("/")
def homepage():
    with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
        print('Root route reached')
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

#time.sleep(20)
#common.logging_initiate()
