from flask import Flask
from flask_login import login_required
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import sys
import dash
import dash_bootstrap_components as dbc

app = Flask(__name__)
app.config.from_prefixed_env() #get config data from environment variables beginning with "FLASK_"

print(app.config)

from FlaskApp.app import auth_real_python
from FlaskApp.app import views

#with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
#    print('__init__.py')

external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app = dash.Dash(server=app,use_pages=True,external_stylesheets=external_stylesheets,pages_folder="") #,routes_pathname_prefix="/dashboard/") #previousy 'routes_pathname_prefix'

from FlaskApp.app import dash_main

for view_func in app.view_functions:
    if view_func.startswith(dash_app.config['routes_pathname_prefix']):
        app.view_functions[view_func] = login_required(app.view_functions[view_func])

# Add SQLAlchemy config for accounting DB
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///accounting.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Flask-Migrate setup
migrate = Migrate(app, db)

# Import models so Alembic sees them
from FlaskApp.app import accounting_db

if __name__ == "__main__":
    dash_main.dash_app.run_server(debug=True)
