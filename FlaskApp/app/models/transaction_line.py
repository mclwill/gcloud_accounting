from datetime import datetime
from FlaskApp.app.accounting_db import db

class TransactionLine(db.Model):
    __tablename__ = 'transaction_lines'
    id = db.Column(db.Integer, primary_key=True)
    transaction_id = db.Column(db.Integer, db.ForeignKey('transactions.id'), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    is_debit = db.Column(db.Boolean, nullable=False)  # True = debit, False = credit
    amount = db.Column(db.Float, nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

    memo = db.Column(db.Text)  # ✅ NEW

