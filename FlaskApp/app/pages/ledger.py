import dash
from dash import html, dash_table, Input, Output, State, callback, dcc, no_update, ctx, clientside_callback
import dash_bootstrap_components as dbc
from dash.dash_table.Format import Format
import pandas as pd
import json
import requests
from flask import session
from urllib.parse import parse_qs
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import FlaskApp.app.common as common
from FlaskApp.app.services.accounts import get_accounts

API_TOKEN = lambda: common.access_secret_version(
    "global_parameters", None, "api_token"
)

dash.register_page(__name__, path="/ledger")


def build_account_dropdown():
    dropdown_list = [
        {"label": acc.name, "value": acc.name}
        for acc in get_accounts()
    ]
    common.logger.debug(f"dropdown values = {dropdown_list}")
    return dropdown_list


def attach_account_names(lines):
    """Ensure each line has an account_name derived from account_id for display."""
    id_to_name = {acc.id: acc.name for acc in get_accounts()}
    for line in lines or []:
        acc_id = line.get("account_id")
        if acc_id is not None:
            line["account_name"] = id_to_name.get(acc_id, line.get("account_name", ""))
    return lines

def is_valid_money(value):
    try:
        d = Decimal(str(value))
        # Reject negatives if you want
        if d < 0:
            return False
        # Ensure max 2 decimal places
        return d.as_tuple().exponent >= -2
    except (InvalidOperation, ValueError):
        return False

# ----------------------------
# Layout
# ----------------------------

editable_columns=[
    #{"name": "Line ID", "id": "id"},
    {"name": "Account", "id": "account_name","editable":True},#,'presentation':'dropdown'},
    {
        "name": "Debit",
        "id": "debit",
        #"type": "numeric",
        "editable" : True,
        "format": Format(precision=2, scheme="f"),
    },
    {
        "name": "Credit",
        "id": "credit",
        #"type": "numeric",
        "editable": True,
        "format": Format(precision=2, scheme="f"),
    },
    {"name": "Memo", "id": "memo", "editable": True},
    {"name": "_account_name_is_editable", "id": "_account_name_is_editable"}

]

non_editable_columns=[
    #{"name": "Line ID", "id": "id"},
    #{"name": "Account", "id": "account_name","editable":False},
    {"name": "Account", "id": "account_link","editable":False,'presentation':'markdown'},
    {
        "name": "Debit",
        "id": "debit",
        #"type": "numeric",
        "editable" : False,
        "format": Format(precision=2, scheme="f"),
    },
    {
        "name": "Credit",
        "id": "credit",
        #"type": "numeric",
        "editable": False,
        "format": Format(precision=2, scheme="f"),
    },
    {"name": "Memo", "id": "memo", "editable": False},
    {"name": "_account_name_is_editable", "id": "_account_name_is_editable"}

]

def blank_transaction_lines():
    return [
        {
            "id": None,
            "account_name": "",
            "debit": None,
            "credit": None,
            "memo": "",
        }
    ]

def new_transaction_lines(ledger_account):
    return [
        {
            "id": None,
            "account_id": ledger_account["account_id"],
            "account_name": ledger_account["account_name"],
            "debit": None,
            "credit": None,
            "memo": "",
        },
        {
            "id": None,
            "account_id": None,
            "account_name": "",
            "debit": None,
            "credit": None,
            "memo": "",
        },
    ]

def layout(account_id=None,account_name=None,txn_id=None, **_):
    if not account_id:
        return html.Div("No account selected")
    common.logger.debug(f"account_name - {account_name}")
    
    entity_name = session.get("current_entity")

    url = common.absolute_url(f"/api/accounts/{account_id}/ledger")

    ledger_resp = requests.get(
        url,
        headers={"X-Internal-Token": API_TOKEN()},
        verify=common.api_verify,
    )

    if ledger_resp.status_code != 200:
        return html.Div("Failed to load ledger")

    ledger_df = pd.DataFrame(ledger_resp.json())

    selected_row = 0
    if txn_id is not None:
        try:
            target = int(txn_id)
            for i, r in enumerate(ledger_df.to_dict("records")):
                if int(r.get("transaction_id")) == target:
                    selected_row = i
                    break
        except Exception:
            pass

    # ✅ Derive account_name safely
    if ledger_df.empty:
        return html.Div("Ledger is empty")

    return html.Div([

        html.Div(
            id="transaction-panel",
            style={"display": "block", "overflow": "visible"},
            children=[
                html.H4("Transaction details"),

                html.Div(
                    [
                        dcc.DatePickerSingle(
                            id="txn-date",
                            date=date.today(),
                            display_format="YYYY-MM-DD",
                        ),
                        dcc.Input(
                            id="txn-description",
                            type="text",
                            placeholder="Transaction description",
                            style={"flex": "1 1 420px", "minWidth": "240px", "maxWidth":"400px"},
                        ),
                        html.Button("➕ New Transaction",id="new-txn-btn",className="primary-btn"),
                        html.Button("Edit", id="edit-btn",disabled=False),
                        html.Button("Save", id="save-btn", disabled=True),
                        html.Button("Cancel", id="cancel-btn", disabled=True),
                        html.Button("Delete", id="delete-btn", disabled=True, className="danger-btn"),
                        html.Button("➕ Add Line",id="add-line-btn",disabled=True),
                        html.Span(id="txn-status", style={"marginLeft": "12px","flex": "1 1 160px", "minWidth": "80px", "maxWidth":"160px"}),
                        
                    ],
                    style={"display": "flex", "gap": "10px",'alignItems':'center'},
                ),
                html.Div(
                    id = 'transaction-lines-wrap',
                    style={"position": "relative", "overflow": "visible"},
                    children = [
                        dash_table.DataTable(
                            id="transaction-lines",
                            editable=True,
                            cell_selectable=True,
                            style_table={"overflowX": "auto"},
                            style_cell_conditional=[
                                {"if": {"column_type": "numeric"}, "textAlign": "right"},

                            ],
                            columns=non_editable_columns,
                            hidden_columns = ['_account_name_is_editable'],
                            markdown_options={"html":True} #,'link_target':'_blank'}
                        )
                    ]
                ),
                dcc.Store(id="txn-active-row", data=None),
                dbc.Popover(
                    id="account-popover",
                    className="acct-popover",
                    target="transaction-lines-wrap",   # attach to the table
                    placement="bottom-start",   # ✅ important: don’t center
                    flip=True,                  # ✅ allow Popper to flip if needed
                    offset=[0, 8],              # ✅ small gap
                    is_open=False,
                    style={"zIndex": 999999, "minWidth": "420px", "maxWidth": "520px"},
                    #trigger="legacy",             # we control open/close via callbacks
                    children=[
                        dbc.PopoverHeader("Select account"),
                        dbc.PopoverBody(
                            dcc.Dropdown(
                                id="account-autocomplete",
                                options=build_account_dropdown(),
                                value=None,
                                searchable=True,
                                clearable=True,
                                placeholder="Type to search…",
                            )
                        ),
                    ],
                )
            ],
        ),
        #Debug displays
        #html.Div(id="popover-open-debug"),

        #html.Details(
        #    [
        #        html.Summary("Debug transaction-lines"),
        #        html.Pre(id="txn-lines-debug", style={"whiteSpace": "pre-wrap"}),
        #    ],
        # ),


        html.Hr(),
        
        dash_table.DataTable(
            id="ledger-table",
            virtualization = False,
            data=ledger_df.to_dict("records"),
            row_selectable="single",
            cell_selectable=True,
            selected_rows=[selected_row],
            sort_action="native",
            filter_action="native",
            style_table={"overflowX": "auto", "overflowY": "auto", "height": "70vh"},
            style_cell_conditional=[
                {"if": {"column_type": "numeric"}, "textAlign": "right"}
            ],
            #style_data_conditional=[
            ##    {
            #        "if": {"state": "selected"},
            #        "backgroundColor": "#C6F2FF",
            #        "border": "1px solid #3399FF",
            #    },
            #    {
            #        "if": {"state": "active"},
            #        "backgroundColor": "#C6F2FF",
            #        "border": "1px solid #3399FF",
            #    },
            #],
            columns=[
                {"name": "Date", "id": "date"},
                {"name": "Description", "id": "description"},
                {
                    "name": "Debit",
                    "id": "debit",
                    "type": "numeric",
                    "format": Format(precision=2, scheme="f"),
                },
                {
                    "name": "Credit",
                    "id": "credit",
                    "type": "numeric",
                    "format": Format(precision=2, scheme="f"),
                },
                {
                    "name": "Balance",
                    "id": "balance",
                    "type": "numeric",
                    "format": Format(precision=2, scheme="f"),
                },
            ],
        ),
        html.Script(
            """
            (function () {
                try {
                    const row = document.getElementById("txn-%s");
                    if (row) {
                        row.scrollIntoView({ behavior: "smooth", block: "center" });
                    }
                } catch (e) {
                    console.log(e);
                }
            })();
            """ % txn_id
        ),


        dcc.ConfirmDialog(
            id="delete-confirm",
            message="Delete this transaction? This cannot be undone.",
        ),


        dcc.Store(id="original-transaction-lines"),
        dcc.Store(id="transaction-mode", data="view"),
        dcc.Store(
            id="current-ledger-account",
            data={
                "account_id": account_id,
                "account_name": account_name,
            },
        ),
        dcc.Store(
            id="transaction-header",
            data={
                "txn_date": None,
                "description": "",
            },
        ),
        dcc.Store(id="ledger-account-id", data=account_id),
        dcc.Store(id="ledger-refresh", data=0),
        dcc.Store(id="last-saved-txn-id", data=int(txn_id) if txn_id is not None else None),
        dcc.Interval(id="ledger-init", interval=250, n_intervals=0, max_intervals=1),
        html.Div(id="ledger-scroll-dummy", style={"display": "none"}),
    ])


# ----------------------------
# Select Row
# ----------------------------

@callback(
    Output("ledger-table", "selected_rows", allow_duplicate=True),
    Output("ledger-table", "active_cell", allow_duplicate=True),
    Output("ledger-table", "selected_cells", allow_duplicate = True),
    Input("ledger-table", "active_cell"),
    prevent_initial_call=True,
)
def select_row_from_any_cell(active_cell):
    if not active_cell:
        common.logger.debug(f'select row 1 : active_cell -> {active_cell}')
        return dash.no_update, dash.no_update, no_update
    common.logger.debug(f'select row 2 : active_cell -> {active_cell}')
    return [active_cell["row"]], None, [{"row":active_cell["row"],"column":0}]

@callback(
    Output("ledger-table", "style_data_conditional"),
    Input("ledger-table", "selected_rows"),
)
def highlight_selected_rows(selected_rows):
    # Base zebra striping + keep Dash's normal active/selected cell behavior
    base = [
        {"if": {"row_index": "odd"}, "backgroundColor": "#FAFAFA"},
        {"if": {"state": "selected"}, "backgroundColor": "#E6F2FF", "border": "1px solid #3399FF"},
        {"if": {"state": "active"},   "backgroundColor": "#E6F2FF", "border": "1px solid #3399FF"},
    ]

    highlight_color = "#E6F2FF"
    border_color = "#3399FF"

    styles = []
    for i in (selected_rows or []):
        styles.append({
            "if": {"row_index": i},
            "backgroundColor": highlight_color,
            "border": f"1px solid {border_color}",
        })

    return base + styles

# ----------------------------
# Load header details
# ----------------------------

@callback(
    Output("txn-date", "date"),
    Output("txn-description", "value"),
    Input("transaction-header", "data"),
    prevent_initial_call=True,
)
def load_header(txn_header):
    if not txn_header['description']:
        txn_header['description'] = ''
    return txn_header['txn_date'], txn_header['description']

# ----------------------------
# Load transaction details
# ----------------------------

@callback(
    Output("transaction-panel", "style"),
    Output("transaction-lines", "data"),
    Output("original-transaction-lines", "data"),  # ✅ NEW
    Output("ledger-table", "selected_rows",allow_duplicate=True),
    Output("transaction-header","data"),
    Input("ledger-table", "selected_rows"),
    Input("ledger-init", "n_intervals"),     # ✅ add this
    State("ledger-table", "data"),
    prevent_initial_call=True,
)
def load_transaction(selected_rows, _init, rows):
    common.logger.debug(f"Load transaction parameters {selected_rows},{rows[0:2]}")

    triggered = ctx.triggered_id

    # If init fired and nothing is selected, default to first row
    if triggered == "ledger-init":
        if not rows:
            return dash.no_update, ...  # whatever your empty case is
        if not selected_rows:
            selected_rows = [0]

    if not selected_rows:
        # ✅ 5 outputs
        return {"display": "none"}, [], None, [0], dash.no_update

    txn_id = rows[selected_rows[0]]["transaction_id"]
    txn_header = {
        "transaction_id":txn_id,
        "txn_date": rows[selected_rows[0]]["date"],
        "description": rows[selected_rows[0]]["description"]
    }

    url = common.absolute_url(f"/api/transactions/{txn_id}")

    resp = requests.get(
        url,
        headers={"X-Internal-Token": API_TOKEN()},
        verify=common.api_verify,
    )
    common.logger.debug(f"Transaction data - {resp}")

    if resp.status_code != 200:
        # ✅ fix typo + correct number of outputs
        return {"display": "block"}, [], None, dash.no_update, dash.no_update

    lines = resp.json()

    for row in lines:
        row["_account_name_is_editable"] = 0
        acc_id = row.get("account_id")
        acc_name = row.get("account_name") or ""
        if acc_id:
            row["account_link"] = (
                f'<a href="/accounts/ledger/{acc_id}?txn_id={txn_id}" '
                f'rel="noopener noreferrer">{acc_name}</a>'
            )
        else:
            row["account_link"] = acc_name

    for row in lines:
        row["_account_name_is_editable"] = 0

    return {"display": "block"}, lines, lines, dash.no_update, txn_header


# ----------------------------
# Reject non-numeric data in Debit and Credit
# ----------------------------

@callback(
    Output("transaction-lines", "data",allow_duplicate=True),
    Input("transaction-lines", "data"),
    State("transaction-lines", "data_previous"),
    prevent_initial_call=True,
)
def reject_non_numeric(data, previous):
    common.logger.debug(f"data_previous = {previous}")
    if not previous:
        return dash.no_update

    for row in data:
        for field in ("debit", "credit"):
            val = row.get(field)

            if val in (None, ""):
                continue

            if not is_valid_money(val):
                common.logger.debug(f"input rejected - {data}")
                return previous  # ⛔ revert immediately

    return dash.no_update

# ----------------------------
# Edit / Cancel toggle
# ----------------------------

from datetime import datetime
import dash

@callback(
    Output("transaction-lines", "columns"),
    Output("edit-btn", "disabled"),
    Output("save-btn", "disabled"),
    Output("cancel-btn", "disabled"),
    Output("txn-status", "children"),
    Output("transaction-lines", "data", allow_duplicate=True),
    Output("transaction-mode", "data"),
    Output("add-line-btn","disabled"),
    Output("account-popover", "is_open", allow_duplicate=True),
    Output("ledger-refresh", "data", allow_duplicate=True),
    Output("last-saved-txn-id", "data", allow_duplicate=True),  # ✅ NEW
    Input("edit-btn", "n_clicks"),
    Input("save-btn", "n_clicks"),
    Input("cancel-btn", "n_clicks"),
    State("transaction-lines", "data"),
    State("original-transaction-lines", "data"),
    State("ledger-table", "selected_rows"),
    State("ledger-table", "data"),
    State("transaction-mode","data"),
    State("txn-date","date"),
    State("txn-description",'value'),
    prevent_initial_call=True,
)
def transaction_edit_controller(
    edit_clicks,
    save_clicks,
    cancel_clicks,
    lines,
    original_lines,
    selected_rows,
    ledger_rows,
    mode,
    txn_date,
    txn_description
):
    trigger = dash.callback_context.triggered_id
    common.logger.debug(f"Trigger = {trigger}")

    # ----------------------------
    # Enter edit mode
    # ----------------------------
    if trigger == "edit-btn":
        for row in lines:
            row["_account_name_is_editable"] = 1

        return (
            editable_columns,
            True, False, False,
            "",
            lines,
            "edit",
            False,
            no_update,
            no_update,          # ledger-refresh
            dash.no_update      # last-saved-txn-id
        )

    # ----------------------------
    # Cancel edit → restore original
    # ----------------------------
    if trigger == "cancel-btn":
        msg = "🛑 New transaction cancelled" if mode == "new" else "🛑 Edit cancelled"
        return (
            non_editable_columns,
            False, True, True,
            msg,
            original_lines,
            None,
            True,
            False,
            no_update,          # ledger-refresh
            dash.no_update      # last-saved-txn-id
        )

    # ----------------------------
    # Save transaction
    # ----------------------------
    if trigger == "save-btn":
        known_txn_id = None

        if mode == "new":
            url = common.absolute_url(f"/api/transactions")

            resp = requests.post(
                url,
                json={"lines": lines, "txn_date": txn_date, "description": txn_description},
                headers={"X-Internal-Token": API_TOKEN()},
                verify=common.api_verify,
            )
        else:
            known_txn_id = ledger_rows[selected_rows[0]]["transaction_id"]
            
            url = common.absolute_url(f"/api/transactions/{known_txn_id}")
            resp = requests.put(
                url,
                json={"lines": lines, "txn_date": txn_date, "description": txn_description},
                headers={"X-Internal-Token": API_TOKEN()},
                verify=common.api_verify,
            )

        if resp.status_code not in (200, 201):
            try:
                msg = resp.json().get("error", resp.text)
            except Exception:
                msg = resp.text

            return (
                editable_columns,
                True, False, False,
                f"❌ Error – {msg}",
                dash.no_update,
                None,
                False,
                no_update,
                no_update,         # ledger-refresh
                dash.no_update     # last-saved-txn-id
            )

        # ✅ Prefer the DB primary key (Transaction.id) because ledger-table uses it
        saved_txn_pk = None
        try:
            payload = resp.json()
            saved_txn_pk = payload.get("id")  # <-- IMPORTANT: use PK
        except Exception:
            saved_txn_pk = None

        # For edits, we already know the PK from the selected ledger row
        if mode != "new" and selected_rows and ledger_rows:
            saved_txn_pk = ledger_rows[selected_rows[0]]["transaction_id"]

        refresh_token = datetime.utcnow().timestamp()

        return (
            non_editable_columns,
            False, True, True,
            "✅ Transaction saved",
            lines,
            None,
            False,
            False,
            refresh_token,
            saved_txn_pk,   # store PK here
        )



        # Fallback
        return (
            non_editable_columns,
            False, True, True,
            "",
            dash.no_update,
            None,
            False,
            False,
            no_update,
            dash.no_update
        )


# ----------------------------
# New transaction
# ----------------------------

@callback(
    Output("transaction-lines", "columns",allow_duplicate=True),
    Output("transaction-panel", "style",allow_duplicate=True),
    Output("transaction-lines", "data",allow_duplicate=True),
    Output("original-transaction-lines", "data",allow_duplicate=True),
    Output("edit-btn", "disabled",allow_duplicate=True),
    Output("save-btn", "disabled",allow_duplicate=True),
    Output("cancel-btn", "disabled",allow_duplicate=True),
    Output("txn-status", "children",allow_duplicate=True),
    Output("transaction-mode", "data",allow_duplicate=True),
    Output("transaction-header","data",allow_duplicate=True),
    Output("add-line-btn","disabled",allow_duplicate=True),
    Input("new-txn-btn", "n_clicks"),
    State("current-ledger-account","data"),
    State("transaction-lines","data"),
    prevent_initial_call=True,
    )
def start_new_transaction(_, ledger_account, txn_lines):
    lines = new_transaction_lines(ledger_account)

    for row in lines:
        row["_account_name_is_editable"] = 1

    return (
        editable_columns,
        {"display": "block"},
        lines,
        txn_lines,
        True,     # editable
        False,    # save enabled
        False,    # cancel enabled
        f"New transaction for {ledger_account['account_name']}",
        "new",
        {
            "txn_date": date.today().isoformat(),
            "description": "",
        },
        False
    )

# ----------------------------
# Delete transaction callbacks
# ----------------------------
@callback(
    Output("delete-btn", "disabled"),
    Input("transaction-mode", "data"),
    Input("ledger-table", "selected_rows"),
)
def toggle_delete_button(mode, selected_rows):
    # Only allow delete when viewing an existing transaction
    if not selected_rows:
        return True
    return mode in ("new", "edit")


@callback(
    Output("delete-confirm", "displayed"),
    Input("delete-btn", "n_clicks"),
    State("delete-btn", "disabled"),
    prevent_initial_call=True,
)
def show_delete_confirm(n, disabled):
    if disabled or not n:
        return no_update
    return True


@callback(
    Output("txn-status", "children", allow_duplicate=True),
    Output("ledger-refresh", "data", allow_duplicate=True),
    Output("ledger-table", "selected_rows", allow_duplicate=True),
    Output("last-saved-txn-id", "data", allow_duplicate=True),
    Output("ledger-table", "active_cell", allow_duplicate=True),
    Input("delete-confirm", "submit_n_clicks"),
    State("ledger-table", "selected_rows"),
    State("ledger-table", "data"),
    State("ledger-refresh", "data"),
    prevent_initial_call=True,
)
def delete_transaction(submit_n, selected_rows, rows, refresh_token):
    if not submit_n:
        return no_update, no_update, no_update, no_update, None

    if not selected_rows or not rows:
        return "No transaction selected", no_update, [], None, None

    txn_id = rows[selected_rows[0]].get("transaction_id")
    if not txn_id:
        return "Couldn't determine transaction id", no_update, [], None, None

    url = common.absolute_url(f"/api/transactions/{txn_id}")
    resp = requests.delete(
        url,
        headers={"X-Internal-Token": API_TOKEN()},
        verify=common.api_verify,
    )

    if resp.status_code != 200:
        msg = "No error information"
        try:
            msg = resp.json().get("error", msg)
        except Exception:
            pass
        return f"Delete failed: {msg}", no_update, no_update, no_update, None

    refresh_token = datetime.utcnow().timestamp()

    return "🗑️ Transaction deleted", refresh_token, no_update, None, None

# ----------------------------
# Add New Line
# ----------------------------

@callback(
    Output("transaction-lines", "data", allow_duplicate=True),
    Input("add-line-btn", "n_clicks"),
    State("transaction-lines", "data"),
    prevent_initial_call=True,
)
def add_transaction_line(_, lines):
    lines.append(
        {
            "id": None,
            "account_id": None,
            "account_name": "",
            "debit": None,
            "credit": None,
            "memo": "",
            "_account_name_is_editable":1
        }
    )
    return lines


# ----------------------------
# open popover when Account cell becomes active
# ----------------------------

from dash import no_update

@callback(
    Output("account-popover", "is_open",allow_duplicate=True),
    Output("account-autocomplete", "value"),
    Output("txn-active-row", "data"),
    Input("transaction-lines", "active_cell"),
    State("transaction-lines", "data"),
    Input("transaction-mode", "data"),
    prevent_initial_call=True,
)
def open_account_popover(active_cell, rows, mode):
    common.logger.debug(f'open account popover parameters {active_cell},{rows},{mode}')
    if not active_cell or mode not in ("new", "edit"):
        return False, None, None
    common.logger.debug('1')
    if active_cell.get("column_id") != "account_name":
        return False, None, None
    common.logger.debug('2')
    row_idx = active_cell.get("row")
    if row_idx is None or not rows or row_idx >= len(rows):
        return False, None, None
    common.logger.debug('3')
    row = rows[row_idx]
    if int(row.get("_account_name_is_editable") or 0) != 1:
        return False, None, None
    common.logger.debug('4')
    return True, row.get("account_name"), row_idx


# ----------------------------
# apply selection to the active row, resolve account_id, close popover
# ----------------------------

@callback(
    Output("transaction-lines", "data", allow_duplicate=True),
    Output("account-popover", "is_open", allow_duplicate=True),
    Output("txn-status", "children", allow_duplicate=True),
    Input("account-autocomplete", "value"),
    State("txn-active-row", "data"),
    State("transaction-lines", "data"),
    State("transaction-header", "data"),   # ✅ ADD
    prevent_initial_call=True,
)
def apply_selected_account(selected_name, row_idx, rows, txn_header):
    common.logger.debug(f'apply_selected_account parameters: {selected_name},{row_idx},{rows}')
    if row_idx is None or not rows or row_idx >= len(rows):
        return no_update, False, no_update
    common.logger.debug(f'apply_selected_account_1')
    current_name = rows[row_idx].get("account_name")

    # Ignore the "prefill" update when opening the popover.
    if selected_name == current_name:
        return no_update, True, no_update
    
    common.logger.debug(f'apply_selected_account_2')
    if not selected_name:
        rows[row_idx]["account_name"] = ""
        rows[row_idx]["account_id"] = None
        return rows, False, "Account cleared"
    common.logger.debug(f'apply_selected_account_3')
    name_to_id = {acc.name: acc.id for acc in get_accounts()}
    account_id = name_to_id.get(selected_name)
    if account_id is None:
        return no_update, True, f"❌ Unknown account: {selected_name}"
    common.logger.debug(f'apply_selected_account_4')
    rows[row_idx]["account_name"] = selected_name
    rows[row_idx]["account_id"] = account_id
    txn_id = (txn_header or {}).get("transaction_id")
    acc_id = rows[row_idx].get("account_id")
    acc_name = rows[row_idx].get("account_name") or ""

    if txn_id and acc_id:
        rows[row_idx]["account_link"] = (
            f'<a href="/accounts/ledger/{acc_id}?txn_id={txn_id}" '
            f'rel="noopener noreferrer">{acc_name}</a>'
        )
    else:
        rows[row_idx]["account_link"] = acc_name

    return rows, False, no_update

# ----------------------------
# Refresh ledger table
# ----------------------------

@callback(
    Output("ledger-table", "data"),
    Output("ledger-table", "selected_rows", allow_duplicate=True),
    #Output("ledger-table", "active_cell", allow_duplicate=True),
    Output("ledger-table", "selected_cells", allow_duplicate=True),
    Input("ledger-refresh", "data"),
    State("ledger-account-id", "data"),
    State("last-saved-txn-id", "data"),
    State("ledger-table","selected_rows"),
    prevent_initial_call=True,
)
def refresh_ledger_table(_refresh_token, account_id, last_saved_txn_id,selected_rows):
    common.logger.debug(f'refresh_ledger_table parameters are : {_refresh_token},{account_id},{last_saved_txn_id},{selected_rows}')
    if not account_id:
        return dash.no_update, dash.no_update, no_update#, None#, []

    url = common.absolute_url(f"/api/accounts/{account_id}/ledger")

    ledger_resp = requests.get(
        url,
        headers={"X-Internal-Token": API_TOKEN()},
        verify=common.api_verify,
    )
    if ledger_resp.status_code != 200:
        return dash.no_update, dash.no_update,no_update#, None#, []

    rows = ledger_resp.json()
    if not rows:
        return [], [], []#, None#, []

    
    common.logger.debug(f"REFRESH ledger first row desc = {rows[0].get('description') if rows else None}")

        # Default selection: top row
    if selected_rows:
        selected_row = selected_rows[0]
    else:
        selected_row = 0

    # ✅ Normalize PK type and find matching row
    target = int(last_saved_txn_id) if last_saved_txn_id is not None else None
    for i, r in enumerate(rows):
        if target is not None and int(r.get("transaction_id")) == target:
            selected_row = i
            break


    # Jump to last saved txn if present
    if last_saved_txn_id is not None:
        for i, r in enumerate(rows):
            if r.get("transaction_id") == last_saved_txn_id:
                selected_row = i
                break
    common.logger.debug(f'selection info : {selected_row}')
    # Clear any old "selected_cells", and set active_cell to the selected row
    return rows, [selected_row],[{'row':selected_row,'column':0}]#, {"row": selected_row, "column_id": "description"}#, []

#----------------------------
# Clientside call back for auto scroll
# ----------------------------

clientside_callback(
    """
    function(selected_rows) {
        if (!selected_rows || selected_rows.length === 0) {
            return window.dash_clientside.no_update;
        }

        const idx = selected_rows[0];

        function attempt(tries) {
            const container = document.querySelector('#ledger-table .dash-spreadsheet-container');
            if (!container) {
                if (tries > 0) setTimeout(() => attempt(tries - 1), 80);
                return;
            }

            // ✅ Robust: some dash-table versions put data-dash-row on TD, not TR
            let row = null;

            // Try TD first (most common)
            const cell = container.querySelector(`td[data-dash-row="${idx}"]`);
            if (cell) {
                row = cell.closest('tr');
            }

            // Fallback: some versions put it on TR
            if (!row) {
                row = container.querySelector(`tr[data-dash-row="${idx}"]`);
            }

            // Final fallback: index-based (last resort)
            if (!row) {
                const rows = container.querySelectorAll('tbody tr');
                row = rows && rows[idx] ? rows[idx] : null;
            }

            if (!row) {
                if (tries > 0) setTimeout(() => attempt(tries - 1), 80);
                return;
            }

            // ---- Scroll container so row is centered ----
            const containerRect = container.getBoundingClientRect();
            const rowRect = row.getBoundingClientRect();

            const currentScrollTop = container.scrollTop;
            const rowTopInContainer = (rowRect.top - containerRect.top) + currentScrollTop;
            const targetScrollTop = rowTopInContainer - (container.clientHeight / 2) + (row.clientHeight / 2);

            container.scrollTo({ top: targetScrollTop, behavior: 'smooth' });

            // ---- Flash (re-triggerable) ----
            row.classList.remove('flash-ledger-row');
            void row.offsetWidth; // reflow to restart animation
            row.classList.add('flash-ledger-row');

            setTimeout(() => row.classList.remove('flash-ledger-row'), 1300);
        }

        setTimeout(() => attempt(8), 0);
        return "";
    }
    """,
    Output("ledger-scroll-dummy", "children"),
    Input("ledger-table", "selected_rows"),
)

#----------------------------
# Debug code
# ----------------------------

@callback(
    Output("txn-lines-debug", "children"),
    Input("ledger-table", "active_cell"),
    Input("ledger-table", "selected_rows"),
    Input("ledger-table", "selected_cells")
)
def dbg_lines(active_cell, selected_rows, selected_cells):
    #sample = (data or [])[:2]
    return json.dumps(
        {
            "active_cell": active_cell,
            "selected_rows" : selected_rows,
            "selected_cells" : selected_cells
        },
        indent=2,
        default=str,
    )

@callback(
    Output("popover-open-debug", "children"),
    Input("account-popover", "is_open"),
)
def dbg_popover_open(is_open):
    return f"popover is_open = {is_open}"
