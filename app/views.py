from app import app

from flask import request, jsonify
from app import common


@app.route("/")
def homepage():
    return "Hi there, how ya doing? Mac 3"

@app.route('/test',methods=['POST','GET'])
def test():
    #args = None
    content = request.get_json(silent=True)
    if content:
        common.send_email(0,'Test Message',str(content),'gary@mclarenwilliams.com.au')
    else:
        common.send_email(0,'Test Message','No content','gary@mclarenwilliams.com.au')

    return 'Something'
