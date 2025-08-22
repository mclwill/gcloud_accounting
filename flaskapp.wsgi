import sys
import logging

logging.basicConfig(stream=sys.stderr)

# Add both the project root and the inner FlaskApp folder to sys.path
sys.path.insert(0, "/var/www/FlaskApp")
sys.path.insert(0, "/var/www/FlaskApp/FlaskApp")

# Import the Dash app
from FlaskApp.app.dash_main import dash_app

# Expose the underlying Flask server to mod_wsgi
application = dash_app.server
