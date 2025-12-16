import pandas as pd
from FlaskApp.app import app, db
from FlaskApp.app.accounting_db import Entity, Account, Transaction
import FlaskApp.app.common as common
import os

def import_accounts():
    # Load Excel file
    common.logger.debug(f"PWD = {os.getcwd()}")
    df = pd.read_csv("FlaskApp/app/assets/JAJG Pty Ltd_Account List.csv")  # adjust path if needed

    # Look for existing entity
    entity = Entity.query.filter_by(name="JAJG Pty Ltd", type="company").first()
    if not entity:
        entity = Entity(name="JAJG Pty Ltd", type="company")
        db.session.add(entity)
        db.session.commit()

    # Cache accounts
    account_cache = {}

    def get_or_create_account(name, type_="Expenses"):
        if name in account_cache:
            return 'Cache'
        account = Account.query.filter_by(entity_id=entity.id, name=name).first()
        if not account:
            account = Account(entity_id=entity.id, name=name, type=type_)
            db.session.add(account)
            db.session.commit()
        account_cache[name] = account
        return 'Created'

    # Iterate rows
    for _, row in df.iterrows():
        account_name = row["Full name"]
        type_ = row['Type']
        description = row['Description']

        if account_name:
            acc = get_or_create_account(account_name,type_)
        else:
            continue
        
    common.logger.debug(f"Imported Accounts for JAJG Pty Ltd = {account_cache.keys()}")

if __name__ == "__main__":
    # Run inside Flask app context
    with app.app_context():
        import_accounts()

