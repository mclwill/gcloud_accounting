from flask import Blueprint, render_template, request, abort, session
from flask_login import login_required
from FlaskApp.app.services.transaction_list import get_transaction_list
from FlaskApp.app.services.accounts import get_accounts
from FlaskApp.app.services.entities import get_entities
from FlaskApp.app.services.transaction_detail import get_transaction_detail
import FlaskApp.app.common as common

bp = Blueprint("transactions_ui", __name__, url_prefix="/transactions")

@bp.route("/")
@login_required
def list():
    account_id = request.args.get("account_id",type=int)
    entity_name = session['current_entity']

    transactions = get_transaction_list(
        entity_name=entity_name,
        status=request.args.get("status"),
        start_date=request.args.get("start_date"),
        end_date=request.args.get("end_date"),
        account_id=account_id,
    )

    common.logger.debug(f"Entities = {get_entities()}")

    return render_template(
        "transactions/list.html",
        transactions=transactions,
        entity_name=entity_name,
        status=request.args.get("status"),
        start_date=request.args.get("start_date"),
        end_date=request.args.get("end_date"),
        account_id=account_id,
        accounts=get_accounts(),
        entities=get_entities()  # we’ll add this next
    )

@bp.route("/<int:transaction_id>")
@login_required
def detail(transaction_id):
    result = get_transaction_detail(transaction_id)

    if not result:
        abort(404)

    transaction, lines = result

    return render_template(
        "transactions/detail.html",
        transaction=transaction,
        lines=lines,
    )

