# FlaskApp/app/routes/transactions_api.py

from __future__ import annotations

from datetime import date as date_cls, datetime
from typing import Any, Optional

from flask import Blueprint, abort, jsonify, request, session
from sqlalchemy import cast
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import Integer

from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.entity import Entity
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.services.transaction_list import get_transaction_lines, get_transaction_list
import FlaskApp.app.common as common

bp = Blueprint("transactions_api", __name__)


def _parse_iso_date(value: Any) -> Optional[date_cls]:
    """Parse an ISO-8601 date (YYYY-MM-DD) into a datetime.date."""
    if value in (None, ""):
        return None
    if isinstance(value, date_cls):
        return value
    if isinstance(value, str):
        try:
            return date_cls.fromisoformat(value)
        except ValueError:
            return None
    return None


def _as_money(v: Any) -> float:
    if v in (None, ""):
        return 0.0
    return float(v)


def _resolve_entity_id_from_session() -> int:
    """
    Session stores entity name (e.g. 'JAJG Pty Ltd'), but DB uses entities.id (int).
    Resolve the name -> id once, and use the integer everywhere.
    """
    entity_name = session.get("current_entity")
    if not entity_name:
        raise ValueError("No current entity in session")

    entity = db.session.query(Entity).filter(Entity.name == entity_name).one_or_none()
    if not entity:
        raise ValueError(f"Unknown entity in session: {entity_name!r}")

    return int(entity.id)


def _next_transaction_id(entity_id: int) -> int:
    """Next sequential transaction_id for this entity_id."""
    max_id = (
        db.session.query(db.func.max(cast(Transaction.transaction_id, Integer)))
        .filter(Transaction.entity_id == entity_id)
        .scalar()
    )
    return int(max_id or 0) + 1


@bp.route("/transactions", methods=["GET"])
def list_transactions():
    token = request.headers.get("X-Internal-Token")
    if token != common.access_secret_version("global_parameters", None, "api_token"):
        abort(403)

    entity_name = session.get("current_entity")
    status = request.args.get("status")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    rows = get_transaction_list(
        entity_name=entity_name,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )

    result = []
    for r in rows:
        result.append(
            {
                "id": r.id,
                "transaction_id": r.transaction_id,
                "date": r.date.isoformat() if r.date else None,
                "description": r.description,
                "transaction_type": r.transaction_type,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                "posted_at": r.posted_at.isoformat() if r.posted_at else None,
            }
        )

    return jsonify(result)


@bp.route("/transactions/<int:transaction_id>", methods=["GET"])
def transaction_detail(transaction_id: int):
    token = request.headers.get("X-Internal-Token")
    if token != common.access_secret_version("global_parameters", None, "api_token"):
        abort(403)

    rows = get_transaction_lines(transaction_id)  # already list[dict]
    return jsonify(rows), 200



@bp.route("/transactions/<int:transaction_id>", methods=["PUT"])
def update_transaction(transaction_id: int):
    token = request.headers.get("X-Internal-Token")
    if token != common.access_secret_version("global_parameters", None, "api_token"):
        abort(403)

    payload = request.get_json(force=True) or {}
    lines = payload.get("lines")
    txn_date_raw = payload.get("txn_date")
    description = payload.get("description") or ""

    if not lines or not isinstance(lines, list) or len(lines) < 2:
        return jsonify({"error": "Transaction must include at least two lines"}), 400

    txn_date = _parse_iso_date(txn_date_raw)
    if not txn_date:
        return jsonify({"error": "Missing or invalid 'txn_date' (expected YYYY-MM-DD)"}), 400

    total_debit = 0.0
    total_credit = 0.0

    for line in lines:
        debit = _as_money(line.get("debit"))
        credit = _as_money(line.get("credit"))

        if debit < 0 or credit < 0:
            return jsonify({"error": "Negative amounts are not allowed"}), 400
        if debit > 0 and credit > 0:
            return jsonify({"error": "A line cannot have both debit and credit"}), 400

        total_debit += debit
        total_credit += credit

    if round(total_debit, 2) != round(total_credit, 2):
        return jsonify({"error": f"Transaction not balanced: debit {total_debit:.2f} vs credit {total_credit:.2f}"}), 400

    txn = db.session.get(Transaction, transaction_id)
    if not txn:
        return jsonify({"error": "Transaction not found"}), 404

    txn.date = txn_date
    txn.description = description

    TransactionLine.query.filter_by(transaction_id=transaction_id).delete()

    for line in lines:
        debit = _as_money(line.get("debit"))
        credit = _as_money(line.get("credit"))

        account_id = line.get("account_id")
        if not account_id:
            return jsonify({"error": "Each line must include 'account_id'"}), 400

        db.session.add(
            TransactionLine(
                transaction_id=txn.id,
                account_id=int(account_id),
                is_debit=debit > 0,
                amount=debit if debit > 0 else credit,
                memo=line.get("memo"),
            )
        )

    db.session.commit()
    return jsonify({"status": "ok","transaction_id": txn.transaction_id, "id": txn.id}), 200


@bp.route("/transactions", methods=["POST"])
def create_transaction():
    token = request.headers.get("X-Internal-Token")
    if token != common.access_secret_version("global_parameters", None, "api_token"):
        abort(403)

    payload = request.get_json(force=True) or {}
    lines = payload.get("lines")
    txn_date_raw = payload.get("txn_date")
    description = payload.get("description") or ""

    if not lines or not isinstance(lines, list) or len(lines) < 2:
        return jsonify({"error": "Transaction must include at least two lines"}), 400

    txn_date = _parse_iso_date(txn_date_raw)
    common.logger.debug(f"txn_date = {txn_date},{type(txn_date)}")
    if not txn_date:
        return jsonify({"error": "Missing or invalid 'txn_date' (expected YYYY-MM-DD)"}), 400

    total_debit = 0.0
    total_credit = 0.0
    for line in lines:
        debit = _as_money(line.get("debit"))
        credit = _as_money(line.get("credit"))

        if debit < 0 or credit < 0:
            return jsonify({"error": "Negative amounts are not allowed"}), 400
        if debit > 0 and credit > 0:
            return jsonify({"error": "A line cannot have both debit and credit"}), 400

        total_debit += debit
        total_credit += credit

    if round(total_debit, 2) != round(total_credit, 2):
        return jsonify({"error": f"Transaction not balanced: debit {total_debit:.2f} vs credit {total_credit:.2f}"}), 400

    try:
        entity_id = _resolve_entity_id_from_session()  # ✅ int id, not name string
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    max_txn = (
        db.session.query(db.func.max(cast(Transaction.transaction_id, Integer)))
        .filter(Transaction.entity_id == entity_id)
        .scalar()
    )
    common.logger.debug(f"entity_id={entity_id!r} existing max txn_id={max_txn!r}")
    common.logger.debug(
        f"existing entity_ids={db.session.query(Transaction.entity_id).distinct().all()}"
    )

    txn = Transaction(
        entity_id=entity_id,
        transaction_id=_next_transaction_id(entity_id),  # ✅ now finds the right rows
        date=txn_date,
        description=description,
        created_at=datetime.utcnow(),
    )

    db.session.add(txn)

    try:
        db.session.flush()  # ensures txn.id is available
    except IntegrityError:
        db.session.rollback()
        # Defensive: if DB still has a global unique constraint, fall back to global max
        global_max = db.session.query(db.func.max(cast(Transaction.transaction_id, Integer))).scalar()
        txn.transaction_id = int(global_max or 0) + 1
        db.session.add(txn)
        db.session.flush()

    for line in lines:
        debit = _as_money(line.get("debit"))
        credit = _as_money(line.get("credit"))

        account_id = line.get("account_id")
        if not account_id:
            return jsonify({"error": "Each line must include 'account_id'"}), 400

        db.session.add(
            TransactionLine(
                transaction_id=txn.id,
                account_id=int(account_id),
                is_debit=debit > 0,
                amount=debit if debit > 0 else credit,
                memo=line.get("memo"),
            )
        )

    db.session.commit()
    return jsonify({"status": "ok", "transaction_id": txn.transaction_id, "id": txn.id}), 201

@bp.route("/transactions/<int:transaction_id>", methods=["DELETE"])
def delete_transaction(transaction_id: int):
    token = request.headers.get("X-Internal-Token")
    if token != common.access_secret_version("global_parameters", None, "api_token"):
        abort(403)

    txn = db.session.get(Transaction, transaction_id)
    if not txn:
        return jsonify({"error": "Transaction not found"}), 404

    # Transaction.lines relationship uses cascade="all, delete-orphan"
    db.session.delete(txn)
    db.session.commit()
    return jsonify({"status": "ok"}), 200

