import pandas as pd
from FlaskApp.app import app, db
from FlaskApp.app.accounting_db import Entity, Account, Transaction, TransactionLine
import FlaskApp.app.common as common
import os

def lookup_account_id(name):
    account = Account.query.filter_by(name=name).first()
    common.logger.debug(f"Account = {name}")
    if account:
        return account.id
    else:
        # Optionally create new account if not found
        new_account = Account(entity_id=1, name=name, type="Expense")
        db.session.add(new_account)
        db.session.flush()
        return new_account.id

def import_gl(entity):
   # Load your parsed GL file
    df = pd.read_csv("FlaskApp/app/assets/General_ledger.csv")

    # Iterate over unique transaction_ids
    for txn_id, group in df.groupby("transaction_id"):
        if txn_id == "check":  # skip unmatched rows
            continue

        # Create Transaction record
        txn = Transaction(
            entity_id=entity,  # replace with actual entity_id
            transaction_id=str(txn_id),
            date=pd.to_datetime(group["Date"].iloc[0]).date(),
            description=group["Memo/Description"].iloc[0],
            transaction_type=group["Transaction Type"].iloc[0],
        )
        db.session.add(txn)
        db.session.flush()  # ensures txn.id is available

        # Create TransactionLine entries for each row in this transaction
        for _, row in group.iterrows():
            # Example: decide debit vs credit based on which column is nonzero
            if row["Debit"] > 0:
                line = TransactionLine(
                    transaction_id=txn.id,
                    account_id=lookup_account_id(row["Account"]),  # implement lookup
                    is_debit=True,
                    amount=row["Debit"],
                )
            if row["Credit"] > 0:
                line = TransactionLine(
                    transaction_id=txn.id,
                    account_id=lookup_account_id(row["Account"]),
                    is_debit=False,
                    amount=row["Credit"],
                )
            db.session.add(line)

    # Commit all inserts
    db.session.commit()

    common.logger.debug("Imported GL for JAJG Pty Ltd")

def lookup_account_id(name):
    account = Account.query.filter_by(name=name).first()
    if account:
        return account.id
    else:
        # Optionally create new account if not found
        new_account = Account(entity_id=1, name=name, type="expense")
        db.session.add(new_account)
        db.session.flush()
        return new_account.id
   

if __name__ == "__main__":
    # Run inside Flask app context
    with app.app_context():
        import_gl('JAJG Pty Ltd')

