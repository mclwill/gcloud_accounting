import pandas as pd
from FlaskApp.app import app, db
from FlaskApp.app.accounting_db import Entity, Account, Transaction
import FlaskApp.app.common as common
import os

def import_gl():
    # Load Excel file
    common.logger.debug(f"PWD = {os.getcwd()}")
    df = pd.read_csv("FlaskApp/app/assets/General_ledger.csv",na_values=["","NA","null"])  # adjust path if needed

    # Look for existing entity
    entity = Entity.query.filter_by(name="JAJG Pty Ltd", type="company").first()
    if not entity:
        entity = Entity(name="JAJG Pty Ltd", type="company")
        db.session.add(entity)
        db.session.commit()

    # Cache accounts
    account_cache = {}

    def get_or_create_account(name, type_="expense"):
        if name in account_cache:
            return account_cache[name]
        account = Account.query.filter_by(entity_id=entity.id, name=name).first()
        if not account:
            raise ValueError(f"Account not found: {name}")  
        account_cache[name] = account
        return account

    # Iterate rows
    for _, row in df.iterrows():
        account_name = row.get("Account")

        # Robust NaN/empty checks
        is_account_missing = (
            pd.isna(account_name) or
            (isinstance(account_name, str) and account_name.strip() == "")
        )
        if is_account_missing:
            # skip rows without a valid account name
            continue
        
        debit = row.get("Debit", 0) or 0
        credit = row.get("Credit", 0) or 0
        amount = debit if debit else credit
        common.logger.debug(f"account_name is : {account_name} and type is {type(account_name)}")

        acc = get_or_create_account(account_name)

        if debit:
            txn = Transaction(
                entity_id=entity.id,
                date=row.get("Date"),
                description=row.get("Description", ""),
                debit_account_id=acc.id,
                credit_account_id=None,   # optional: suspense/offset
                amount=debit,
                transaction_type=row.get("Transaction Type", "Journal")
            )
        elif credit:
            txn = Transaction(
                entity_id=entity.id,
                date=row.get("Date"),
                description=row.get("Description", ""),
                debit_account_id=None,
                credit_account_id=acc.id,
                amount=credit,
                transaction_type=row.get("Transaction Type", "Journal")
            )
        else:
            continue

        db.session.add(txn)

    db.session.commit()
    print("Imported GL for JAJG Pty Ltd")

if __name__ == "__main__":
    # Run inside Flask app context
    with app.app_context():
        import_gl()

