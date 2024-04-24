from app import app
import sys
with open('/var/log/cd-uphance/app.log', 'a') as sys.stdout:
    print('run.py')
if __name__ == "__main__":
    app.run()
