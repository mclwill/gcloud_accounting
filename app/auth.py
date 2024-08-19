import flask_login
import flask
import FlaskApp.app.common as common
from FlaskApp.app import app

login_manager = flask_login.LoginManager()
login_manager.init_app(app)

class User(flask_login.UserMixin):
    pass

users = common.get_users()

@login_manager.user_loader
def load_user(username):
    if username not in users:
        return

    user = User()
    user.id = username
    return user

@login_manager.request_loader
def request_loader(request):
    user = request.form.get('username')
    if user not in users:
        return

    user = User()
    user.id = user
    return user

@app.route('/login', methods=['GET', 'POST'])
def login():
    if flask.request.method == 'GET':
        return '''
               <form action='login' method='POST'>
                <input type='text' name='username' id='username' placeholder='username'/>
                <input type='password' name='password' id='password' placeholder='password'/>
                <input type='submit' name='submit'/>
               </form>
               '''

    user = flask.request.form['username']
    
    common.logger.info(str(users) + ' : ' + user + ' : ' + flask.request.form['password'])
    common.logger.info(str(users[user]))

    if user in users and flask.request.form['password'] == users[user]:
        user = User()
        user.id = user
        flask_login.login_user(user)
        return redirect(flask.url_for('protected'))

    return 'Bad login'


@app.route('/protected')
@flask_login.login_required
def protected():
    return 'Logged in as: ' + flask_login.current_user.id

@app.route('/logout')
def logout():
    flask_login.logout_user()
    return 'Logged out'

@login_manager.unauthorized_handler
def unauthorized_handler():
    return 'Unauthorized', 401