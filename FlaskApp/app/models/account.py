from FlaskApp.app.accounting_db import db

class Account(db.Model):
    __tablename__ = 'accounts'

    id = db.Column(db.Integer, primary_key=True)
    entity_id = db.Column(db.Integer, db.ForeignKey('entities.id'), nullable=False)

    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.String(50), nullable=False)
    description = db.Column(db.String(100))

    __table_args__ = (
        db.UniqueConstraint("name", "type", name="_account_name_type_uc"),
    )

    entity = db.relationship(
        'Entity',
        back_populates='accounts',
    )

    entries = db.relationship('TransactionLine', backref='account', lazy=True)


