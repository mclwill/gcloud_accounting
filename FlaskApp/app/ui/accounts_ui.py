from flask import Blueprint, render_template, abort, request, session
from flask_login import login_required
from FlaskApp.app.models.account import Account
from FlaskApp.app.services.account_list import get_account_list
from FlaskApp.app.services.entities import get_entities
from FlaskApp.app.services.accounts import get_account

import FlaskApp.app.common as common

bp = Blueprint("accounts_ui", __name__, url_prefix="/accounts")

@bp.route("/")
@login_required
def list():
    # Loads inside base.html with the dashboard top panel
    return render_template(
        "accounts/list_dash.html",
        dash_src="/dash/accounts",
        entity_name=session.get("current_entity"),
        entities=get_entities(),
    )

@bp.route("/ledger/<int:account_id>")
@login_required
def ledger(account_id):
    common.logger.debug(f"RAW QUERY STRING: {request.query_string}")
    common.logger.debug(f"FULL URL: {request.url}")

    entity_name = session['current_entity']
    common.logger.debug(f"entity_name = {entity_name}")
    account = get_account(account_id)
    txn_id = request.args.get("txn_id")
    common.logger.debug(f"/accounts/ledger route with account_id = {account_id}, entity_name = {entity_name}")
    return render_template(
        "accounts/ledger.html",
        account=account,
        entity_name=entity_name,
        txn_id=txn_id,   # ✅ ADD
    )

@bp.route("/<int:account_id>")
@login_required
def detail(account_id):
    account = Account.query.get_or_404(account_id)
    return render_template("accounts/detail.html", account=account)

