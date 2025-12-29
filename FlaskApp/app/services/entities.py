from sqlalchemy import func, case
from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.entity import Entity

def get_entities():
    return (
        db.session.query(Entity)
        .order_by(Entity.id)
        .all()
    )