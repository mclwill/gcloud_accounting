"""create csv import reviews

Revision ID: b1c2d3e4f5a6
Revises: 9f1c2e3a4b5c
Create Date: 2026-01-01 07:56:01

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b1c2d3e4f5a6"
down_revision = "9f1c2e3a4b5c"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "csv_import_reviews",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("entity_id", sa.Integer(), sa.ForeignKey("entities.id"), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False, server_default="banktivity"),
        sa.Column("fingerprint", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("linked_transaction_id", sa.Integer(), sa.ForeignKey("transactions.id"), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("entity_id", "source", "fingerprint", name="uq_entity_source_fingerprint"),
    )
    op.create_index("ix_csv_import_reviews_entity_source", "csv_import_reviews", ["entity_id", "source"])


def downgrade():
    op.drop_index("ix_csv_import_reviews_entity_source", table_name="csv_import_reviews")
    op.drop_table("csv_import_reviews")
