from flask import Flask
from flask_login import login_required
import sys

app = Flask(__name__)


from FlaskApp.app import auth
from FlaskApp.app import views

#with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
#    print('__init__.py')

dash_app = dash.Dash(server=app,use_pages=True,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/") #previousy 'routes_pathname_prefix'

from FlaskApp.app import dash_main

for view_func in app.view_functions:
    if view_func.startswith(dash_app.config['routes_pathname_prefix']):
        app.view_functions[view_func] = login_required(app.view_functions[view_func])

if __name__ == "__main__":
    dash_main.dash_app.run_server(debug=True)
