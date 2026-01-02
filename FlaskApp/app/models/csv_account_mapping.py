# FlaskApp/app/models/csv_account_mapping.py

from datetime import datetime
from FlaskApp.app.accounting_db import db


class CsvAccountMapping(db.Model):
    """Maps an external CSV account/category name to an internal Account.

    'source' lets you support multiple import formats in future (e.g. 'banktivity').
    """

    __tablename__ = "csv_account_mappings"
    __table_args__ = (
        db.UniqueConstraint("entity_id", "source", "csv_account_name", name="uq_entity_source_csv_account"),
    )

    id = db.Column(db.Integer, primary_key=True)

    entity_id = db.Column(db.Integer, db.ForeignKey("entities.id"), nullable=False)
    source = db.Column(db.String(50), nullable=False, default="banktivity")

    csv_account_name = db.Column(db.String(200), nullable=False)

    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

    account = db.relationship("Account")
