from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.account import Account

import FlaskApp.app.common as common

def get_account_ledger(account_id):
    account = (
        db.session.query(Account)
        .filter(Account.id == account_id)
        .one_or_none()
    )

    if not account:
        return None

    rows = (
        db.session.query(
            Transaction.date,
            Transaction.transaction_id,
            Transaction.description,
            Transaction.id.label("transaction_pk"),
            TransactionLine.is_debit,
            TransactionLine.amount,
        )
        .join(Transaction, TransactionLine.transaction_id == Transaction.id)
        .filter(TransactionLine.account_id == account_id)
        .order_by(Transaction.date, Transaction.id)
        .all()
    )

    common.logger.debug(f"Ledger rows for account {account_id}: {len(rows)}")
    
    return account, rows
