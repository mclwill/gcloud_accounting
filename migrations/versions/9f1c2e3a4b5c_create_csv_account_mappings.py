"""Create csv_account_mappings table

Revision ID: 9f1c2e3a4b5c
Revises: 23783ad3c535
Create Date: 2025-12-31 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "9f1c2e3a4b5c"
down_revision = "23783ad3c535"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "csv_account_mappings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("entity_id", sa.Integer(), sa.ForeignKey("entities.id"), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False, server_default="banktivity"),
        sa.Column("csv_account_name", sa.String(length=200), nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("(CURRENT_TIMESTAMP)")),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("entity_id", "source", "csv_account_name", name="uq_entity_source_csv_account"),
    )
    op.create_index(
        "ix_csv_account_mappings_entity_source",
        "csv_account_mappings",
        ["entity_id", "source"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_csv_account_mappings_entity_source", table_name="csv_account_mappings")
    op.drop_table("csv_account_mappings")
