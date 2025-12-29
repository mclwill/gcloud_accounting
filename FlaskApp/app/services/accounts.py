from sqlalchemy import func, case
from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.account import Account

def get_accounts():
    return (
        db.session.query(Account)
        .order_by(Account.id)
        .all()
    )

def get_account(account_id):
	"""
	Return a single Account by ID, or None if not found.
	"""
	return (
	    db.session.query(Account)
	    .filter(Account.id == account_id)
	    .one_or_none()
	)