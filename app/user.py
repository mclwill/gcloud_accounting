import threading
import FlaskApp.app.auth_real_python as arp
#from . import db
from flask_login import UserMixin
import FlaskApp.app.common as common

lock = threading.Lock()

class User(UserMixin):
    def __init__(self, id_, name, email, profile_pic):
        self.id = id_
        self.name = name
        self.email = email
        self.profile_pic = profile_pic

    @staticmethod
    def get(user_id):
        #sql_query = """SELECT name FROM sqlite_master  WHERE type='table';"""
        #arp.cursor.execute(sql_query)
        #common.logger.debug(str(arp.cursor.fetchall()))
        sql = "SELECT * FROM user WHERE id = ?"
        lock.acquire(True)
        lockarp.cursor.execute(sql,(user_id,))
        user = arp.cursor.fetchone()
        lock.release()

        #db = get_db()
        #user = db.execute(
        #    "SELECT * FROM user WHERE id = ?", (user_id,)
        #).fetchone()
        if not user:
            return None

        user = User(
            id_=user[0], name=user[1], email=user[2], profile_pic=user[3]
        )
        return user

    @staticmethod
    def create(id_, name, email, profile_pic):
        sql = "INSERT INTO user (id, name, email, profile_pic) VALUES(?,?,?,?)"
        lock.acquire(True)
        arp.cursor.execute(sql,(id_, name, email, profile_pic))
        arp.conn.commit()
        lock.release()
        #db.execute(
        #    "INSERT INTO user (id, name, email, profile_pic)"
        #    " VALUES (?, ?, ?, ?)",
        #    (id_, name, email, profile_pic),
        #)
        #db.commit()

'''
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True) # primary keys are required by SQLAlchemy
    email = db.Column(db.String(100), unique=True)
    password = db.Column(db.String(100))
    name = db.Column(db.String(1000))
'''