#from app import app
#import sys
#with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
#    print('run.py')
#if __name__ == "__main__":
#    app.run(debug=True)

from FlaskApp.app import dash_main

if __name__ == "__main__":
    dash_main.dash_app.run_server(debug=True)
