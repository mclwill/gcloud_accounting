# FlaskApp/app/pages/accounts.py
import dash
from dash import html, dash_table, dcc, Input, Output, callback

from flask import session
from sqlalchemy import func, case

from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.account import Account
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.entity import Entity

import FlaskApp.app.common as common

dash.register_page(__name__, path="/accounts")


def _account_rows(entity_name: str, account_type: str | None):
    debit_sum = func.sum(
        case((TransactionLine.is_debit == True, TransactionLine.amount), else_=0)
    ).label("debit_total")

    credit_sum = func.sum(
        case((TransactionLine.is_debit == False, TransactionLine.amount), else_=0)
    ).label("credit_total")

    q = (
        db.session.query(
            Account.id.label("id"),
            Account.name.label("name"),
            Account.type.label("type"),
            debit_sum,
            credit_sum,
        )
        .select_from(Account)
        .outerjoin(TransactionLine, TransactionLine.account_id == Account.id)
        .outerjoin(Transaction, TransactionLine.transaction_id == Transaction.id)
        .outerjoin(Entity, Transaction.entity_id == Entity.id)
        .filter(Account.entity.has(name=entity_name))
        .group_by(Account.id)
        .order_by(Account.type, Account.name)
    )

    if account_type and account_type != "__ALL__":
        q = q.filter(Account.type == account_type)

    rows = []
    for r in q.all():
        debit = float(r.debit_total or 0.0)
        credit = float(r.credit_total or 0.0)
        bal = debit - credit

        rows.append(
            {
                "type": r.type or "",
                # Markdown link opens the ledger page in same iframe
                "name": f"[{r.name}](/accounts/ledger/{r.id})",
                "debit_total": debit,
                "credit_total": credit,
                "balance": bal,
            }
        )
    return rows


def _account_type_options(entity_name: str):
    q = (
        db.session.query(Account.type)
        .filter(Account.entity.has(name=entity_name))
        .distinct()
        .order_by(Account.type)
    )
    types = [t[0] for t in q.all() if t[0]]
    return [{"label": "All types", "value": "__ALL__"}] + [
        {"label": t, "value": t} for t in types
    ]


layout = html.Div(
    style={"padding": "16px"},
    children=[
        #html.H3("Accounts"),
        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap"},
            children=[
                html.Div("Account type:"),
                dcc.Dropdown(
                    id="acct-type-filter",
                    options=[],
                    value=None,
                    clearable=False,
                    style={"minWidth": "260px"},
                ),
            ],
        ),
        html.Div(style={"height": "10px"}),
        dash_table.DataTable(
            id="accounts-table",
            columns=[
                {"name": "Type", "id": "type"},
                {"name": "Name", "id": "name", "presentation": "markdown"},
                {"name": "Balance", "id": "balance", "type": "numeric", "format": {"specifier": ",.2f"}},
                {"name": "Debit", "id": "debit_total", "type": "numeric", "format": {"specifier": ",.2f"}},
                {"name": "Credit", "id": "credit_total", "type": "numeric", "format": {"specifier": ",.2f"}},
                
            ],
            data=[],
            sort_action="native",
            filter_action='none',
            page_action="native",
            page_size=25,
            markdown_options={"link_target": "_top"},  # ✅ open in same tab
            style_table={"overflowX": "auto"},
            style_cell={"padding": "6px", "fontFamily": "system-ui"},
            style_cell_conditional=[
                {"if": {"column_id": c}, "textAlign": "right"} for c in ["debit_total", "credit_total", "balance"]
            ],
        ),
        html.Div(id="accounts-status", style={"marginTop": "8px"}),
    ],
)


@callback(
    Output("acct-type-filter", "options"),
    Output("acct-type-filter", "value"),
    Output("accounts-table", "data"),
    Output("accounts-status", "children"),
    Input("acct-type-filter", "value"),
)
def load_accounts(selected_type):
    common.logger.debug(f'load_accounts parameters - {selected_type}')
    entity_name = session.get("current_entity")
    if not entity_name:
        return (
            [{"label": "All types", "value": "__ALL__"}],
            "__ALL__",
            [],
            "No current entity selected.",
        )

    # Populate dropdown options (based on current entity)
    opts = _account_type_options(entity_name)
    values = [o["value"] for o in opts]

    # ✅ Default to Bank if present
    if selected_type is None:
        if "Bank" in values:
            selected_type = "Bank"
        else:
            selected_type = "__ALL__"

    # If selected type no longer exists, fall back safely
    if selected_type not in values:
        selected_type = "__ALL__"

    data = _account_rows(entity_name, selected_type)
    return opts, selected_type, data, f"{len(data)} account(s)"

