from FlaskApp.app.accounting_db import db

class Entity(db.Model):
    __tablename__ = 'entities'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    type = db.Column(db.String(50))  # company, trust, etc.
    description = db.Column(db.String(250))

    __table_args__ = (
        db.UniqueConstraint("name", "type", name="_name_type_uc"),
    )

    accounts = db.relationship(
        'Account',
        back_populates='entity',
        lazy=True,
        cascade='all, delete-orphan',
    )
