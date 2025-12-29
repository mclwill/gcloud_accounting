# run this from within 'flask shell' - see below
'''
(venv) gary@accounting:/var/www/FlaskApp$ export FLASK_APP=FlaskApp.app
(venv) gary@accounting:/var/www/FlaskApp$ flask shell
<Config {'DEBUG': False, 'TESTING': False, 'PROPAGATE_EXCEPTIONS': None, 'SECRET_KEY': None, 'PERMANENT_SESSION_LIFETIME': datetime.timedelta(days=31), 'USE_X_SENDFILE': False, 'SERVER_NAME': None, 'APPLICATION_ROOT': '/', 'SESSION_COOKIE_NAME': 'session', 'SESSION_COOKIE_DOMAIN': None, 'SESSION_COOKIE_PATH': None, 'SESSION_COOKIE_HTTPONLY': True, 'SESSION_COOKIE_SECURE': False, 'SESSION_COOKIE_SAMESITE': None, 'SESSION_REFRESH_EACH_REQUEST': True, 'MAX_CONTENT_LENGTH': None, 'SEND_FILE_MAX_AGE_DEFAULT': None, 'TRAP_BAD_REQUEST_ERRORS': None, 'TRAP_HTTP_EXCEPTIONS': False, 'EXPLAIN_TEMPLATE_LOADING': False, 'PREFERRED_URL_SCHEME': 'http', 'TEMPLATES_AUTO_RELOAD': None, 'MAX_COOKIE_SIZE': 4093, 'APP': 'FlaskApp.app', 'RUN_FROM_CLI': True}>
[12-14 06:36:09] p38469 {/var/www/FlaskApp/FlaskApp/app/common.py:141} DEBUG - Attempting to start SMTP logging
[12-14 06:36:09] p38469 {/var/www/FlaskApp/FlaskApp/app/common.py:159} DEBUG - Cross Docks - Uphance API: Logging started for File, Stream and SMTP logging
[12-14 06:36:09] p38469 {/var/www/FlaskApp/FlaskApp/app/common.py:509} DEBUG - Initiate logging done
[12-14 06:36:10] p38469 {/var/www/FlaskApp/FlaskApp/app/common.py:475} INFO - 
Logger Info for aemery Uphance initiated and Uphance token expires on: 2026-04-15
Message Id: 19b1b92ea91c0edd
[12-14 06:36:12] p38469 {/var/www/FlaskApp/FlaskApp/app/common.py:478} DEBUG - {'Status': 'Updated'}
[12-14 06:36:13] p38469 {/var/www/FlaskApp/FlaskApp/app/common.py:475} INFO - 
Logger Info for two-ts Uphance initiated and Uphance token expires on: 2026-04-15
Message Id: 19b1b92f32599b42
[12-14 06:36:14] p38469 {/var/www/FlaskApp/FlaskApp/app/common.py:478} DEBUG - {'Status': 'Updated'}
Python 3.8.10 (default, Mar 18 2025, 20:04:55) 
[GCC 9.4.0] on linux
App: FlaskApp.app
Instance: /var/www/FlaskApp/instance
>>> 
'''
from . import db, accounting_db, app

with app.app_context():
	db.create_all()