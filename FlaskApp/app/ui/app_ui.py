from flask import Blueprint, request, redirect, session, url_for
from flask_login import login_required

bp = Blueprint("app_ui", __name__)

SESSION_ENTITY_KEY = "current_entity"

@bp.route("/set-entity", methods=["POST"])
@login_required
def set_entity():
    entity = request.form.get("entity")
    if entity:
        session[SESSION_ENTITY_KEY] = entity
    return redirect(request.referrer or url_for("homepage"))
