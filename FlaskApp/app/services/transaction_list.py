from sqlalchemy import func, case
from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.account import Account
from FlaskApp.app.models.entity import Entity

def get_transaction_list(
    entity_id=None,
    entity_name=None,
    status=None,          # "draft", "posted", or None
    start_date=None,
    end_date=None,
    account_id=None,      # NEW
):
    """
    Returns one row per transaction with debit/credit totals.
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

    account_names = func.group_concat(
        func.distinct(Account.name)
    ).label("account_names")


    query = (
        db.session.query(
            Transaction.id,
            Transaction.transaction_id,
            Transaction.date,
            Transaction.description,
            Transaction.transaction_type,
            Transaction.created_at,
            Transaction.posted_at,
            debit_sum,
            credit_sum,
            account_names
        )
        .select_from(Transaction)  # 🔑 anchor the query
        .outerjoin(TransactionLine, TransactionLine.transaction_id == Transaction.id)
        .outerjoin(Account, TransactionLine.account_id == Account.id)
        .group_by(Transaction.id)
        .order_by(Transaction.date.desc(), Transaction.created_at.desc())
    )

    if entity_name:
        query = query.join(Transaction.entity).filter(Entity.name == entity_name)

    if status == "draft":
        query = query.filter(Transaction.posted_at.is_(None))
    elif status == "posted":
        query = query.filter(Transaction.posted_at.isnot(None))

    if account_id:
        query = query.filter(TransactionLine.account_id == account_id)

    if start_date:
        query = query.filter(Transaction.date >= start_date)

    if end_date:
        query = query.filter(Transaction.date <= end_date)

    return query.all()

def get_transaction_lines(transaction_id):
    """
    Returns all lines for a single transaction.
    Used for ledger drill-down and editing.
    """

    rows = (
        db.session.query(
            TransactionLine.id.label("transaction_line_id"),
            TransactionLine.transaction_id,
            TransactionLine.account_id,
            Account.name.label("account_name"),
            case(
                (TransactionLine.is_debit == True, TransactionLine.amount),
                else_=0,
            ).label("debit"),
            case(
                (TransactionLine.is_debit == False, TransactionLine.amount),
                else_=0,
            ).label("credit"),
            TransactionLine.memo,
        )
        .join(Account, TransactionLine.account_id == Account.id)
        .filter(TransactionLine.transaction_id == transaction_id)
        .order_by(TransactionLine.id)
        .all()
    )

    return [
        {
            "id": r.transaction_line_id,
            "transaction_id": r.transaction_id,
            "account_id": r.account_id,
            "account_name": r.account_name,
            "debit": float(r.debit or 0),
            "credit": float(r.credit or 0),
            "memo": r.memo,
        }
        for r in rows
    ]

