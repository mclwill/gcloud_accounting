from flask import Flask
import sys
import dash
from dash import Dash, html, dcc

app = Flask(__name__)


from FlaskApp.app import auth
from FlaskApp.app import views
from FlaskApp.app.pages import dashboard

with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
    print('__init__.py')

external_stylesheets = [dbc.themes.BOOTSTRAP,'https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app = dash.Dash(server=app,use_pages=True,external_stylesheets=external_stylesheets,routes_pathname_prefix="/dashboard/") #previousy 'routes_pathname_prefix'

dash_app.layout = html.Div([
    dash.page_container
])

if __name__ == "__main__":
    dash_app.run_server(debug=True)
