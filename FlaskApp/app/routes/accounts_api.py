from flask import Blueprint, jsonify, request, abort, session
from flask_login import login_required
from FlaskApp.app.services.transaction_list import get_transaction_list
from FlaskApp.app.utils.money import money  # adjust import if needed
import FlaskApp.app.common as common

from FlaskApp.app import app

bp = Blueprint("accounts_api", __name__)

@bp.route("/accounts/<int:account_id>/ledger", methods=["GET"])
def account_ledger(account_id):
    token = request.headers.get("X-Internal-Token")
    #common.logger.debug(f"app.config = {app.config}")
    if (not app.config['API_DEBUG']) and (token != common.access_secret_version("global_parameters", None, "api_token")):
        abort(403)

    entity_name = session.get("current_entity")
    common.logger.debug(f"account_id={account_id}, entity_name={entity_name}")

    rows = get_transaction_list(
        account_id=account_id,
        entity_name=entity_name,
    )

    result = []
    balance = 0.0

    for r in rows:
        debit = money(r.debit_total)
        credit = money(r.credit_total)
        balance += credit - debit

        result.append({
            "transaction_id": r.id,              # ✅ REQUIRED
            "date": r.date.isoformat(),
            "description": r.description,
            "debit": debit,
            "credit": credit,
            "balance": balance,
        })

    return jsonify(result)

