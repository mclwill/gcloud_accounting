from datetime import date
from . import db  # import the db object from __init__.py

class Entity(db.Model):
    __tablename__ = 'entities'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    type = db.Column(db.String(50))  # company, trust, etc.
    # Enforce uniqueness across name + type
    __table_args__ = (
        db.UniqueConstraint("name", "type", name="_name_type_uc"),
    )
    description = db.Column(db.String(250))

    accounts = db.relationship('Account', backref='entity', lazy=True)
    transactions = db.relationship('Transaction', backref='entity', lazy=True)


class Account(db.Model):
    __tablename__ = 'accounts'
    id = db.Column(db.Integer, primary_key=True)
    entity_id = db.Column(db.Integer, db.ForeignKey('entities.id'), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.String(50), nullable=False)  # asset, liability, income, expense


class Transaction(db.Model):
    __tablename__ = 'transactions'
    id = db.Column(db.Integer, primary_key=True)
    entity_id = db.Column(db.Integer, db.ForeignKey('entities.id'), nullable=False)
    date = db.Column(db.Date, default=date.today, nullable=False)
    description = db.Column(db.String(200))
    debit_account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    credit_account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    # NEW FIELD
    transaction_type = db.Column(db.String(50))  # e.g. 'Journal', 'Payment', 'Receipt'

    debit_account = db.relationship('Account', foreign_keys=[debit_account_id])
    credit_account = db.relationship('Account', foreign_keys=[credit_account_id])
