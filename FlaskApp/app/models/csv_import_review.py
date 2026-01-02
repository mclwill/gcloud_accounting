# FlaskApp/app/models/csv_import_review.py

from datetime import datetime
from FlaskApp.app.accounting_db import db


class CsvImportReview(db.Model):
    """Stores per-entity review decisions for imported CSV transactions.

    We record a stable fingerprint for a CSV transaction (typically based on date, amount,
    mapped asset account, and payee/details). This lets us remember that a CSV row was
    reviewed and should be skipped in future imports (e.g. confirmed duplicate).
    """

    __tablename__ = "csv_import_reviews"
    __table_args__ = (
        db.UniqueConstraint("entity_id", "source", "fingerprint", name="uq_entity_source_fingerprint"),
    )

    id = db.Column(db.Integer, primary_key=True)

    entity_id = db.Column(db.Integer, db.ForeignKey("entities.id"), nullable=False)
    source = db.Column(db.String(50), nullable=False, default="banktivity")

    # SHA1/hex fingerprint of the CSV transaction
    fingerprint = db.Column(db.String(40), nullable=False)

    # One of: 'duplicate', 'imported', 'ignored'
    status = db.Column(db.String(20), nullable=False)

    # Optional link to an existing transaction that this CSV row was deemed to duplicate
    linked_transaction_id = db.Column(db.Integer, db.ForeignKey("transactions.id"), nullable=True)

    reviewed_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
