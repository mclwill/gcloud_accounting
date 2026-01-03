from flask import Blueprint, request, redirect, session, url_for, flash
from flask_login import login_required
from sqlalchemy.exc import IntegrityError

from FlaskApp.app.services.entities import create_entity

bp = Blueprint("app_ui", __name__)

SESSION_ENTITY_KEY = "current_entity"

@bp.route("/set-entity", methods=["POST"])
@login_required
def set_entity():
    entity = request.form.get("entity")
    if entity:
        session[SESSION_ENTITY_KEY] = entity
    return redirect(request.referrer or url_for("homepage"))

@bp.route("/entities/new", methods=["POST"])
@login_required
def add_entity():
    """Create a new entity and switch the current session entity to it."""
    name = request.form.get("entity_name", "").strip()
    type_ = request.form.get("entity_type", "").strip() or None
    description = request.form.get("entity_description", "").strip() or None

    try:
        ent = create_entity(name=name, type_=type_, description=description)
        session[SESSION_ENTITY_KEY] = ent.name
        flash(f"Entity created: {ent.name}", "success")
    except ValueError as e:
        flash(str(e), "error")
    except IntegrityError:
        # Rollback the failed transaction so the session can continue
        from FlaskApp.app.accounting_db import db
        db.session.rollback()
        flash("That entity already exists (same name and type).", "error")
    except Exception:
        # Be defensive—don't break navigation on unexpected issues
        from FlaskApp.app.accounting_db import db
        db.session.rollback()
        flash("Could not create entity due to an unexpected error.", "error")

    return redirect(request.referrer or url_for("homepage"))
