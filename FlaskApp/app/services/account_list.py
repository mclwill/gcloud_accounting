from sqlalchemy import func, case
from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.account import Account
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.entity import Entity


def get_account_list(
    entity_name=None,
):
    """
    Returns one row per account with debit / credit totals.
    """

    debit_sum = func.sum(
        case(
            (TransactionLine.is_debit == True, TransactionLine.amount),
            else_=0,
        )
    ).label("debit_total")

    credit_sum = func.sum(
        case(
            (TransactionLine.is_debit == False, TransactionLine.amount),
            else_=0,
        )
    ).label("credit_total")

    query = (
        db.session.query(
            Account.id,
            #Account.code,
            Account.name,
            debit_sum,
            credit_sum,
        )
        .select_from(Account)
        .outerjoin(TransactionLine, TransactionLine.account_id == Account.id)
        .outerjoin(Transaction, TransactionLine.transaction_id == Transaction.id)
        .outerjoin(Entity, Transaction.entity_id == Entity.id)
        .group_by(Account.id)
        #.order_by(Account.code)
    )

    if entity_name:
        query = query.filter(Account.entity.has(name=entity_name))


    return query.all()
