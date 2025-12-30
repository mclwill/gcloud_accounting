from flask import Blueprint, jsonify, request, abort, session
from flask_login import login_required
from FlaskApp.app.services.transaction_list import get_transaction_list
from FlaskApp.app.utils.money import money  # adjust import if needed
import FlaskApp.app.common as common
from FlaskApp.app.services.balance_sheet_report import TYPE_TO_SECTION as BS_TYPE_TO_SECTION
from FlaskApp.app.services.pnl_report import TYPE_TO_SECTION as PNL_TYPE_TO_SECTION
from FlaskApp.app.models.account import Account


from FlaskApp.app import app

bp = Blueprint("accounts_api", __name__)

@bp.route("/accounts/<int:account_id>/ledger", methods=["GET"])
def account_ledger(account_id):
    token = request.headers.get("X-Internal-Token")
    #common.logger.debug(f"app.config = {app.config}")
    if (not app.config['API_DEBUG']) and (token != common.access_secret_version("global_parameters", None, "api_token")):
        abort(403)

    entity_name = session.get("current_entity")

    account = Account.query.get_or_404(account_id)

    acct_type = (account.type or "").strip()
    section = BS_TYPE_TO_SECTION.get(acct_type) or PNL_TYPE_TO_SECTION.get(acct_type)

    # Debit-normal accounts
    debit_normal = section in ("Assets", "Expenses", "Cost of Goods Sold")

    common.logger.debug(f"account_id={account_id}, entity_name={entity_name}")

    rows_list = get_transaction_list(
        account_id=account_id,
        entity_name=entity_name,
    )

    balances_by_txn_id = {}
    running = 0.0

    for r in reversed(rows_list):  # oldest -> newest
        debit = money(r.debit_total)
        credit = money(r.credit_total)

        if debit_normal:
            # Assets / Expenses / COGS
            running += (debit - credit)
        else:
            # Liabilities / Equity / Income
            running += (credit - debit)

        balances_by_txn_id[r.id] = running

    result = []
    for r in rows_list:  # newest -> oldest
        result.append({
            "transaction_id": r.id,
            "date": r.date.isoformat(),
            "description": r.description,
            "debit": money(r.debit_total),
            "credit": money(r.credit_total),
            "balance": balances_by_txn_id.get(r.id, 0.0),
        })

    return jsonify(result)

