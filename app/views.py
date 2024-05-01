from FlaskApp.app import app
#import time
import sys
from flask import request, jsonify
import FlaskApp.app.common as common


@app.route("/")
def homepage():
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


with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
    print('Views.py end')

