"""force posted transactions only

Revision ID: fb8e792038e6
Revises: 23783ad3c535
Create Date: 2026-01-03 05:30:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fb8e792038e6'
down_revision = '23783ad3c535'
branch_labels = None
depends_on = None


def upgrade():
    # Backfill any existing "draft" transactions (posted_at IS NULL)
    op.execute("""
        UPDATE transactions
        SET posted_at = COALESCE(posted_at, created_at, CURRENT_TIMESTAMP)
        WHERE posted_at IS NULL
    """)

    # Enforce posted_at present for all rows going forward
    op.alter_column(
        'transactions',
        'posted_at',
        existing_type=sa.DateTime(),
        nullable=False,
        server_default=sa.text('CURRENT_TIMESTAMP'),
    )


def downgrade():
    # Allow NULL again (reverting server_default as well)
    op.alter_column(
        'transactions',
        'posted_at',
        existing_type=sa.DateTime(),
        nullable=True,
        server_default=None,
    )
