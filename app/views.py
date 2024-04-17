from app import app

from flask import request, jsonify
from app import common


@app.route("/")
def homepage():
    return "Hi there, how ya doing? Mac 3"

@app.route('/test')
def test():
    args = None
    if request.args:
        content = request.get_json(silent=True)
        common.send_email(0,'Test Message',str(content),'gary@mclarenwilliams.com.au')
    else:
        common.send_email(0,'Test Message','No content','gary@mclarenwilliams.com.au')

    return 'Something'
