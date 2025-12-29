from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.account import Account

def get_transaction_detail(transaction_id):
    transaction = (
        db.session.query(Transaction)
        .filter(Transaction.id == transaction_id)
        .one_or_none()
    )

    if not transaction:
        return None

    lines = (
        db.session.query(
            TransactionLine,
            Account
        )
        .join(Account, TransactionLine.account_id == Account.id)
        .filter(TransactionLine.transaction_id == transaction_id)
        .order_by(TransactionLine.id)
        .all()
    )

    return transaction, lines
