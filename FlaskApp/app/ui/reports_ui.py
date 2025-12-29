from urllib.parse import urlencode

from flask import Blueprint, render_template, request, session
from flask_login import login_required

bp = Blueprint("reports_ui", __name__, url_prefix="/reports")


@bp.route("/pnl", methods=["GET"])
@login_required
def pnl():
    entity_name = session.get("current_entity")

    # Optional query params passed from header:
    start = request.args.get("start")
    end = request.args.get("end")

    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if entity_name:
        params["entity_name"] = entity_name

    dash_src = "/dash/pnl"
    if params:
        dash_src += "?" + urlencode(params)

    return render_template(
        "reports/pnl.html",
        entity_name=entity_name,
        dash_src=dash_src,
    )


@bp.route("/balance-sheet", methods=["GET"])
@login_required
def balance_sheet():
    entity_name = session.get("current_entity")

    # Optional query param passed from header:
    asof = request.args.get("asof")

    params = {}
    if asof:
        params["asof"] = asof
    if entity_name:
        params["entity_name"] = entity_name

    dash_src = "/dash/balance-sheet"
    if params:
        dash_src += "?" + urlencode(params)

    return render_template(
        "reports/balance_sheet.html",
        entity_name=entity_name,
        dash_src=dash_src,
    )
