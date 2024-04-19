from FlaskApp.app import app

from flask import request, jsonify
import FlaskApp.app.common as common


@app.route("/")
def homepage():
    return "Nothing to see here"

@app.route('/test',methods=['POST'])
def test():
    #args = None
    content = request.get_json(silent=True)
    if content:
        common.send_email(0,'Test Message',str(content),'gary@mclarenwilliams.com.au')
    else:
        common.send_email(0,'Test Message','No content','gary@mclarenwilliams.com.au')

    return 'Test Processed - check email'
