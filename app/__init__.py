from flask import Flask
import dash
import dash_html_components as html
import sys

app = Flask(__name__)

from FlaskApp.app import views

with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
    print('__init__.py')

dash_app = dash.Dash(server=app,routes_pathname_prefix="/dashboard/")

dash_app.layout = html.Div("This is the Dash app")

if __name__ == "__main__":
    dash_app.run_server(debug=True)
