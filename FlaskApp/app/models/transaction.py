# FlaskApp/app/models/transaction.py

from datetime import datetime, date
from FlaskApp.app.accounting_db import db


class Transaction(db.Model):
    __tablename__ = "transactions"
    __table_args__ = (
        db.UniqueConstraint("entity_id", "transaction_id", name="uq_entity_transaction_id"),
    )

    id = db.Column(db.Integer, primary_key=True)

    entity_id = db.Column(db.Integer, db.ForeignKey("entities.id"), nullable=False)

    # Unique per entity_id (via composite UniqueConstraint above)
    transaction_id = db.Column(db.Integer, nullable=False, index=True)

    date = db.Column(db.Date, default=date.today, nullable=False)
    description = db.Column(db.String(200))
    transaction_type = db.Column(db.String(50))  # e.g. 'Journal', 'Payment', 'Receipt'

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)
    posted_at = db.Column(db.DateTime)

    entity = db.relationship("Entity", backref="transactions")

    lines = db.relationship(
        "TransactionLine",
        backref="transaction",
        cascade="all, delete-orphan",
        lazy=True,
    )

    @property
    def is_posted(self):
        return self.posted_at is not None
