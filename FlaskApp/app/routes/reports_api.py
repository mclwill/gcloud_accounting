# FlaskApp/app/routes/reports_api.py
from __future__ import annotations

from datetime import date
from io import BytesIO

from flask import Blueprint, abort, jsonify, request, session, send_file

import FlaskApp.app.common as common
from FlaskApp.app.models.entity import Entity
from FlaskApp.app.services.pnl_report import build_pnl
from FlaskApp.app.services.pnl_excel import build_pnl_workbook
from FlaskApp.app.services.balance_sheet_report import build_balance_sheet
from FlaskApp.app.services.balance_sheet_excel import build_balance_sheet_workbook
from FlaskApp.app.accounting_db import db

bp = Blueprint("reports_api", __name__)


def _require_token():
    token = request.headers.get("X-Internal-Token")
    if token != common.access_secret_version("global_parameters", None, "api_token"):
        abort(403)


@bp.route("/pnl", methods=["GET"])
def pnl_json():
    _require_token()

    entity_name = session.get("current_entity")
    entity = db.session.query(Entity).filter(Entity.name == entity_name).one_or_none()
    if not entity:
        return jsonify({"error": "No current entity"}), 400

    # Example query params: ?p1_start=2023-07-01&p1_end=2024-06-30&p2_start=2024-07-01&p2_end=2025-06-30
    periods = []
    i = 1
    while True:
        s = request.args.get(f"p{i}_start")
        e = request.args.get(f"p{i}_end")
        lbl = request.args.get(f"p{i}_label") or f"Period {i}"
        if not s or not e:
            break
        periods.append((lbl, date.fromisoformat(s), date.fromisoformat(e)))
        i += 1

    if not periods:
        return jsonify({"error": "Provide at least p1_start and p1_end"}), 400

    return jsonify(build_pnl(entity.id, periods))


@bp.route("/pnl.xlsx", methods=["GET"])
def pnl_excel():
    _require_token()

    entity_name = session.get("current_entity")
    entity = db.session.query(Entity).filter(Entity.name == entity_name).one_or_none()
    if not entity:
        return jsonify({"error": "No current entity"}), 400

    periods = []
    i = 1
    while True:
        s = request.args.get(f"p{i}_start")
        e = request.args.get(f"p{i}_end")
        lbl = request.args.get(f"p{i}_label") or f"Period {i}"
        if not s or not e:
            break
        periods.append((lbl, date.fromisoformat(s), date.fromisoformat(e)))
        i += 1

    if not periods:
        return jsonify({"error": "Provide at least p1_start and p1_end"}), 400

    pnl = build_pnl(entity.id, periods)

    # Title range string like your example
    report_title_range = f"{periods[0][1].strftime('%B %Y')} - {periods[-1][2].strftime('%B %Y')}"
    wb = build_pnl_workbook(pnl, entity.name, report_title_range)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    return send_file(
        bio,
        as_attachment=True,
        download_name="profit_and_loss.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@bp.route("/balance_sheet", methods=["GET"])
def balance_sheet_json():
    _require_token()

    entity_name = session.get("current_entity")
    entity = db.session.query(Entity).filter(Entity.name == entity_name).one_or_none()
    if not entity:
        return jsonify({"error": "No current entity"}), 400

    cols = []
    i = 1
    while True:
        d = request.args.get(f"c{i}_date")
        lbl = request.args.get(f"c{i}_label") or f"As of {i}"
        if not d:
            break
        cols.append((lbl, date.fromisoformat(d)))
        i += 1

    if not cols:
        return jsonify({"error": "Provide at least c1_date"}), 400

    return jsonify(build_balance_sheet(entity.id, cols))


@bp.route("/balance_sheet.xlsx", methods=["GET"])
def balance_sheet_excel():
    _require_token()

    entity_name = session.get("current_entity")
    entity = db.session.query(Entity).filter(Entity.name == entity_name).one_or_none()
    if not entity:
        return jsonify({"error": "No current entity"}), 400

    cols = []
    i = 1
    while True:
        d = request.args.get(f"c{i}_date")
        lbl = request.args.get(f"c{i}_label") or f"As of {i}"
        if not d:
            break
        cols.append((lbl, date.fromisoformat(d)))
        i += 1

    if not cols:
        return jsonify({"error": "Provide at least c1_date"}), 400

    bs = build_balance_sheet(entity.id, cols)

    subtitle = f"As of {cols[-1][1].strftime('%B %d, %Y')}"
    wb = build_balance_sheet_workbook(bs, entity.name, subtitle)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    return send_file(
        bio,
        as_attachment=True,
        download_name="balance_sheet.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

