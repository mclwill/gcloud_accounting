from flask import Flask
import sys

app = Flask(__name__)


from FlaskApp.app import views

with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
    print('__init__.py')

if __name__ == "__main__":
    app.run(debug=True)

with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
    print('run.py in __init__.py')
