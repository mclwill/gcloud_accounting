from flask import Flask
import sys

app = Flask(__name__)

from FlaskApp.app import views
from FlaskApp.app import dashboard

with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
    print('__init__.py')

if __name__ == "__main__":
    dashboard.dash_app.run_server(debug=True)
