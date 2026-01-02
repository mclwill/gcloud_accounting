from __future__ import annotations

from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import load_only
from sqlalchemy import func, select

from flask import Blueprint, abort, flash, redirect, render_template, request, session, url_for, current_app
from flask_login import login_required

from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.account import Account
from FlaskApp.app.models.entity import Entity
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.csv_account_mapping import CsvAccountMapping
from FlaskApp.app.models.csv_import_review import CsvImportReview

from FlaskApp.app.services.transaction_list import get_transaction_list
from FlaskApp.app.services.accounts import get_accounts
from FlaskApp.app.services.balance_sheet_report import ASSET_TYPES
from FlaskApp.app.services.entities import get_entities
from FlaskApp.app.services.transaction_detail import get_transaction_detail
from FlaskApp.app.services.banktivity_import import parse_banktivity_csv

bp = Blueprint("transactions_ui", __name__, url_prefix="/transactions")

def _current_entity_id() -> int:
    name = session.get("current_entity")
    if not name:
        # fall back to default configured in app init
        name = "JAJG Pty Ltd"
        session["current_entity"] = name

    ent = db.session.query(Entity).filter(Entity.name == name).one_or_none()
    if not ent:
        abort(400, description=f"Unknown entity '{name}'")
    return int(ent.id)


@bp.route("/")
@login_required
def list():
    entity_name = session.get("current_entity")

    account_id = request.args.get("account_id", type=int)
    status = request.args.get("status") or None
    start_date = request.args.get("start_date") or None
    end_date = request.args.get("end_date") or None
    q = request.args.get("q") or None
    amount = request.args.get("amount", type=float)

    transactions = get_transaction_list(
        entity_name=entity_name,
        status=status,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        search_text=q,
        search_amount=amount,
    )

    return render_template(
        "transactions/list.html",
        transactions=transactions,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        q=q,
        amount=amount,
        accounts=[a for a in get_accounts() if a.type in ASSET_TYPES],
        all_accounts=get_accounts(),
        entities=get_entities(),
    )


@bp.route("/<int:transaction_id>")
@login_required
def detail(transaction_id: int):
    result = get_transaction_detail(transaction_id)
    if not result:
        abort(404)

    transaction, lines = result
    return render_template(
        "transactions/detail.html",
        transaction=transaction,
        lines=lines,
    )

def _csv_txn_fingerprint(dt: date, signed_amount: float, asset_account_id: int, term: str) -> str:
    """Stable fingerprint for remembering reviewed CSV rows."""
    import hashlib
    amt = round(float(signed_amount), 2)
    norm = (term or "").strip().lower()
    raw = f"{dt.isoformat()}|{amt:.2f}|{int(asset_account_id)}|{norm}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _csv_fingerprint_for_txn(t, asset_account_id: int) -> str:
    """
    Canonical fingerprint for a parsed CSV transaction.
    Must be used anywhere we persist/lookup CsvImportReview rows.
    """
    if not getattr(t, "lines", None):
        raise ValueError("CSV txn has no lines")
    ln = t.lines[0]
    signed_amt = ln.amount if ln.is_debit else -ln.amount
    signed_amt = round(float(signed_amt), 2)
    term = (t.payee or t.details or "").strip()
    term = term[:64]
    return _csv_txn_fingerprint(t.date, signed_amt, int(asset_account_id), term)



def _csv_total_signed(t) -> float:
    """Compute signed total from CSV transaction lines (robust across parsers)."""
    total = 0.0
    for ln in getattr(t, "lines", []) or []:
        amt = float(getattr(ln, "amount", 0) or 0)
        is_debit = bool(getattr(ln, "is_debit", True))
        total += amt if is_debit else -amt
    return round(total, 2)


def _csv_total_abs(t) -> float:
    return abs(_csv_total_signed(t))


def _find_match_candidates(*, entity_id: int, tx_date: date, amount_abs: float, tol: float = 0.005, limit: int = 5) -> List[dict]:
    """Find possible existing DB matches for a CSV row (date + amount)."""
    if not tx_date or amount_abs <= 0:
        return []

    # Find candidate transaction ids by matching any line amount around amount_abs on same date.
    cand_ids = (
        db.session.query(Transaction.id)
        .join(TransactionLine)
        .filter(Transaction.entity_id == entity_id)
        .filter(Transaction.date == tx_date)
        .filter(TransactionLine.amount.between(amount_abs - tol, amount_abs + tol))
        .distinct()
        .limit(limit)
        .all()
    )
    cand_ids = [int(x[0]) for x in cand_ids]
    if not cand_ids:
        return []

    # Load candidates and an asset-account name (if any)
    txns = (
        db.session.query(Transaction)
        .options(load_only(Transaction.id, Transaction.description, Transaction.date))
        .filter(Transaction.id.in_(cand_ids))
        .all()
    )
    # Map id -> "other side" account(s) (non-asset accounts) for display.
    # Your template expects the key name "asset_account"; we keep that key but populate it
    # with the non-asset account(s) involved in the candidate transaction.
    rows = (
        db.session.query(TransactionLine.transaction_id, Account.name)
        .join(Account, Account.id == TransactionLine.account_id)
        .filter(TransactionLine.transaction_id.in_(cand_ids))
        .filter(~Account.type.in_(ASSET_TYPES))
        .all()
    )
    other_names: Dict[int, str] = {}
    for tid, name in rows:
        tid = int(tid)
        if not name:
            continue
        existing = other_names.get(tid)
        if existing:
            # keep unique names, preserve order
            parts = existing.split(", ")
            if name not in parts:
                other_names[tid] = existing + ", " + name
        else:
            other_names[tid] = name

    out = []
    tx_by_id = {t.id: t for t in txns}
    for tid in cand_ids:
        t = tx_by_id.get(tid)
        if not t:
            continue
        out.append(
            {
                "id": int(t.id),
                "description": t.description,
                "asset_account": other_names.get(t.id),
            }
        )
    return out

# ------------------------------
# CSV Import (Banktivity)
# ------------------------------

def _upsert_csv_review(*, entity_id: int, source: str, fingerprint: str, status: str, linked_transaction_id: Optional[int]) -> bool:
    """Insert or update a CsvImportReview row. Returns True if a new row was inserted."""
    existing = (
        db.session.query(CsvImportReview)
        .filter(CsvImportReview.entity_id == entity_id)
        .filter(CsvImportReview.source == source)
        .filter(CsvImportReview.fingerprint == fingerprint)
        .one_or_none()
    )
    if existing is None:
        db.session.add(
            CsvImportReview(
                entity_id=entity_id,
                source=source,
                fingerprint=fingerprint,
                status=status,
                linked_transaction_id=linked_transaction_id,
                reviewed_at=datetime.utcnow(),
            )
        )
        return True

    # Keep the most informative state (duplicate beats imported), but allow setting linked id on imported.
    existing.status = status
    if linked_transaction_id is not None:
        existing.linked_transaction_id = linked_transaction_id
    existing.reviewed_at = datetime.utcnow()
    return False


@bp.route("/import", methods=["GET", "POST"])
@login_required
def import_csv():
    """Preview Banktivity CSV, allow mapping, duplicate marking, and importing."""
    entity_id = _current_entity_id()

    show_mappings = (request.values.get("show_mappings") == "1")
    show_duplicates = (request.values.get("show_duplicates") == "1")
    show_unmapped = (request.values.get("show_unmapped") == "1")

    # Date filter
    start_date_str = request.values.get("start_date") or ""
    start_date: Optional[date] = None
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

    csv_path = request.values.get("csv_path") or ""

    # Recently imported transaction ids (stored in session on POST).
    last_imported_ids = session.pop("csv_last_imported_ids", None)

    # Upload CSV
    if request.method == "POST" and "csv_file" in request.files and request.files["csv_file"].filename:
        f = request.files["csv_file"]
        upload_dir = current_app.instance_path + "/csv_uploads"
        import os
        os.makedirs(upload_dir, exist_ok=True)
        safe_name = f"banktivity_{entity_id}_{int(datetime.utcnow().timestamp())}.csv"
        csv_path = os.path.join(upload_dir, safe_name)
        f.save(csv_path)
        flash("CSV uploaded.", "success")
        return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

    # Load mappings for this entity/source
    mappings: Dict[str, int] = {
        m.csv_account_name: int(m.account_id)
        for m in (
            db.session.query(CsvAccountMapping)
            .filter(CsvAccountMapping.entity_id == entity_id)
            .filter(CsvAccountMapping.source == "banktivity")
            .all()
        )
    }

    # Parse CSV (if provided)
    txns = []
    if csv_path:
        try:
            txns = parse_banktivity_csv(csv_path, start_date=start_date)
        except Exception as e:
            flash(f"Failed to parse CSV: {e}", "error")
            txns = []

    txn_by_id = {getattr(t, "trn_no", None): t for t in txns}

    # Handle imports (and duplicate marking)
    if request.method == "POST" and request.form.get("action") == "import_selected":
        # Selected transactions to import (may be empty if user is only marking duplicates)
        selected_ids = {int(x) for x in request.form.getlist("import_trn_no") if x}

        # Confirmed duplicates (persist regardless of whether anything is imported)
        dup_ids = {int(x) for x in request.form.getlist("dup_trn_no") if x}

        if not csv_path:
            flash("No CSV uploaded.", "error")
            return redirect(url_for("transactions_ui.import_csv"))

        # Re-parse to avoid trusting form fields
        txns = parse_banktivity_csv(csv_path, start_date=start_date)
        txn_by_id = {t.trn_no: t for t in txns}

        # Refresh mappings (in case they changed in another tab)
        mappings = {
            m.csv_account_name: int(m.account_id)
            for m in (
                db.session.query(CsvAccountMapping)
                .filter(CsvAccountMapping.entity_id == entity_id)
                .filter(CsvAccountMapping.source == "banktivity")
                .all()
            )
        }

        # Validate required mappings for any selected row
        unmapped_needed = set()
        for trn_no in selected_ids | dup_ids:
            t = txn_by_id.get(trn_no)
            if not t or not t.lines:
                continue
            csv_acct = t.lines[0].csv_account_name
            if csv_acct not in mappings:
                unmapped_needed.add(csv_acct)

        if unmapped_needed:
            flash(
                "Cannot proceed yet. Please map these CSV accounts first: " + ", ".join(sorted(unmapped_needed)),
                "error",
            )
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

        # Persist duplicate reviews FIRST so they survive any later rollback due to missing confirmations.
        saved_dups = 0
        for trn_no in sorted(dup_ids):
            t = txn_by_id.get(trn_no)
            if not t or not t.lines:
                continue
            asset_id = mappings.get(t.lines[0].csv_account_name)
            if not asset_id:
                continue
            fp = _csv_fingerprint_for_txn(t, asset_id)
            inserted = _upsert_csv_review(
                entity_id=entity_id,
                source="banktivity",
                fingerprint=fp,
                status="duplicate",
                linked_transaction_id=None,
            )
            if inserted:
                saved_dups += 1

        if saved_dups:
            db.session.commit()
            flash(f"Remembered {saved_dups} duplicate(s).", "success")

        # Never import rows marked as duplicates.
        selected_ids = {x for x in selected_ids if x not in dup_ids}

        # If a CSV transaction has possible DB matches, require an explicit confirmation checkbox.
        confirmed = {int(x) for x in request.form.getlist("confirm_trn_no") if x}

        tol = 0.005
        next_txn_id = (
            db.session.query(db.func.max(Transaction.transaction_id))
            .filter(Transaction.entity_id == entity_id)
            .scalar()
            or 0
        )

        imported = 0
        imported_txn_ids: List[int] = []
        blocked_for_review: List[int] = []

        for trn_no in sorted(selected_ids):
            t = txn_by_id.get(trn_no)
            if not t or not t.lines:
                continue

            # Optional counter-account from dropdown
            counter_account_id = None
            counter_val = request.form.get(f"counter__{trn_no}") or ""
            if counter_val.strip():
                try:
                    counter_account_id = int(counter_val)
                except ValueError:
                    counter_account_id = None

            # Detect possible existing matches: same date, similar amount (any line)
            amt = _csv_total_abs(t)
            candidates_exist = False
            if amt > 0:
                candidates_exist = (
                    db.session.query(Transaction.id)
                    .join(TransactionLine)
                    .filter(Transaction.entity_id == entity_id)
                    .filter(Transaction.date == t.date)
                    .filter(TransactionLine.amount.between(amt - tol, amt + tol))
                    .distinct()
                    .first()
                    is not None
                )

            if candidates_exist and trn_no not in confirmed:
                blocked_for_review.append(trn_no)
                continue

            # Create transaction
            next_txn_id += 1
            txn = Transaction(
                entity_id=entity_id,
                transaction_id=next_txn_id,
                date=t.date,
                description=(t.payee or t.details or "")[:255],
                status="cleared",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.session.add(txn)
            db.session.flush()  # txn.id available

            # Remember imported fingerprint so we can hide re-imports from preview.
            asset_id = mappings.get(t.lines[0].csv_account_name)
            if asset_id:
                fp = _csv_fingerprint_for_txn(t, asset_id)
                _upsert_csv_review(
                    entity_id=entity_id,
                    source="banktivity",
                    fingerprint=fp,
                    status="imported",
                    linked_transaction_id=txn.id,
                )

            # Create mapped asset line(s)
            for ln in t.lines:
                acct_id = mappings[ln.csv_account_name]
                db.session.add(
                    TransactionLine(
                        transaction_id=txn.id,
                        account_id=acct_id,
                        is_debit=ln.is_debit,
                        amount=float(ln.amount),
                        memo=ln.memo,
                    )
                )

            # Optional counter line (balanced)
            if counter_account_id and t.lines:
                ref_ln = t.lines[0]
                db.session.add(
                    TransactionLine(
                        transaction_id=txn.id,
                        account_id=counter_account_id,
                        is_debit=(not ref_ln.is_debit),
                        amount=float(ref_ln.amount),
                        memo="Auto counter (CSV import)",
                    )
                )

            imported += 1
            imported_txn_ids.append(txn.id)

        if blocked_for_review:
            db.session.rollback()
            flash(
                "Possible duplicates found. Please review and tick 'Confirm' for: "
                + ", ".join(str(x) for x in blocked_for_review),
                "error",
            )
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

        db.session.commit()
        if imported_txn_ids:
            session["csv_last_imported_ids"] = imported_txn_ids
        if imported:
            flash(f"Imported {imported} transaction(s).", "success")
        return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

    # Handle mapping saves
    if request.method == "POST" and request.form.get("action") == "save_mappings":
        updates = 0
        csv_names = request.form.getlist("csv_names")
        for idx, csv_name in enumerate(csv_names):
            acct_id = request.form.get(f"map_to__{idx}") or ""
            if not acct_id.strip():
                continue
            acct_id = int(acct_id)

            existing = (
                db.session.query(CsvAccountMapping)
                .filter(CsvAccountMapping.entity_id == entity_id)
                .filter(CsvAccountMapping.source == "banktivity")
                .filter(CsvAccountMapping.csv_account_name == csv_name)
                .one_or_none()
            )
            if existing:
                if int(existing.account_id) != acct_id:
                    existing.account_id = acct_id
                    updates += 1
            else:
                db.session.add(
                    CsvAccountMapping(
                        entity_id=entity_id,
                        source="banktivity",
                        csv_account_name=csv_name,
                        account_id=acct_id,
                    )
                )
                updates += 1

        db.session.commit()
        flash(f"Saved {updates} mapping(s).", "success")
        return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

    # ------------------------------
    # Render preview
    # ------------------------------

    # Only show transactions whose *asset* CSV account has been mapped for this entity.
    preview_txns = []
    unmapped_txns = []
    unmapped_accounts = set()

    for t in txns:
        if not getattr(t, "lines", None):
            continue
        csv_acct = t.lines[0].csv_account_name
        if csv_acct in mappings:
            preview_txns.append(t)
        else:
            unmapped_accounts.add(csv_acct)
            unmapped_txns.append(t)

    missing_mappings = sorted(unmapped_accounts)

    # Hide CSV rows previously reviewed (duplicate OR already-imported) for this entity/source.
    reviewed_duplicates = []
    hidden_reviewed_txns = []  # list of dicts {txn, status, linked_transaction_id}
    if preview_txns:
        fp_by_trn: Dict[int, str] = {}
        fps: List[str] = []
        for t in preview_txns:
            asset_id = mappings.get(t.lines[0].csv_account_name)
            if not asset_id:
                continue
            fp = _csv_fingerprint_for_txn(t, asset_id)
            fp_by_trn[t.trn_no] = fp
            fps.append(fp)

        reviewed: Dict[str, CsvImportReview] = {}
        if fps:
            reviewed = {
                r.fingerprint: r
                for r in (
                    db.session.query(CsvImportReview)
                    .filter(CsvImportReview.entity_id == entity_id)
                    .filter(CsvImportReview.source == "banktivity")
                    .filter(CsvImportReview.fingerprint.in_(fps))
                    .all()
                )
            }

        filtered = []
        for t in preview_txns:
            fp = fp_by_trn.get(t.trn_no)
            r = reviewed.get(fp) if fp else None
            if r and r.status in {"duplicate", "imported"}:
                if r.status == "duplicate":
                    reviewed_duplicates.append({"trn_no": t.trn_no, "linked_transaction_id": r.linked_transaction_id})
                hidden_reviewed_txns.append({"txn": t, "status": r.status, "linked_transaction_id": r.linked_transaction_id})
                continue
            filtered.append(t)
        preview_txns = filtered

    # Suggest a likely non-asset counterparty account based on historical transactions.
    suggestions: Dict[int, Optional[dict]] = {}
    if csv_path and preview_txns:
        term_for_trn: Dict[int, str] = {}
        unique_terms: Dict[str, Optional[dict]] = {}

        for t in preview_txns:
            term = (t.payee or t.details or "").strip()
            if not term:
                continue
            term = term[:64]
            term_for_trn[t.trn_no] = term
            unique_terms.setdefault(term, None)

        non_asset_accounts_sq = (
            db.session.query(Account.id)
            .filter(Account.entity_id == entity_id)
            .filter(~Account.type.in_(ASSET_TYPES))
            .subquery()
        )

        for term in unique_terms.keys():
            res = (
                db.session.query(Account.id, Account.name, func.count(TransactionLine.id).label("cnt"))
                .join(TransactionLine, TransactionLine.account_id == Account.id)
                .join(Transaction, Transaction.id == TransactionLine.transaction_id)
                .filter(Transaction.entity_id == entity_id)
                .filter(Transaction.description.ilike(f"%{term}%"))
                .filter(Account.id.in_(select(non_asset_accounts_sq.c.id)))
                .group_by(Account.id, Account.name)
                .order_by(func.count(TransactionLine.id).desc())
                .limit(1)
                .first()
            )
            if res:
                unique_terms[term] = {"account_id": int(res.id), "account_name": res.name, "count": int(res.cnt)}

        for trn_no, term in term_for_trn.items():
            suggestions[trn_no] = unique_terms.get(term)

    # Build preview rows for template
    preview_rows = []
    for t in preview_txns:
        if not getattr(t, "lines", None):
            continue

        asset_csv = t.lines[0].csv_account_name
        mapped_account_id = mappings.get(asset_csv)

        mapped_account_name = None
        if mapped_account_id:
            mapped_account_name = db.session.query(Account.name).filter(Account.id == mapped_account_id).scalar()

        amount_signed = _csv_total_signed(t)
        amount_abs = _csv_total_abs(t)

        match_candidates = _find_match_candidates(
            entity_id=entity_id,
            tx_date=t.date,
            amount_abs=amount_abs,
            tol=0.005,
            limit=5,
        )
        match_count = len(match_candidates)

        preview_rows.append(
            {
                "trn_no": t.trn_no,
                "date": t.date,
                "type": getattr(t, "type", None),
                "payee": getattr(t, "payee", None),
                "lines": getattr(t, "lines", []),
                "mapped_account_id": mapped_account_id,
                "mapped_account_name": mapped_account_name,
                "amount": amount_signed,
                "suggested": suggestions.get(t.trn_no),
                "match_count": match_count,
                "match_candidates": match_candidates,
            }
        )

    # Optional: surface hidden items for review (unmapped CSV accounts / remembered duplicates).
    hidden_duplicate_rows = []
    hidden_imported_rows = []
    if show_duplicates and hidden_reviewed_txns:
        for item in hidden_reviewed_txns:
            t = item["txn"]
            row = {
                "trn_no": t.trn_no,
                "date": t.date,
                "payee": getattr(t, "payee", None),
                "details": getattr(t, "details", None),
                "amount": float(_csv_total_signed(t)),
                "asset_csv_account": t.lines[0].csv_account_name if getattr(t, "lines", None) else None,
                "status": item["status"],
                "linked_transaction_id": item.get("linked_transaction_id"),
            }
            if item["status"] == "duplicate":
                hidden_duplicate_rows.append(row)
            else:
                hidden_imported_rows.append(row)

    hidden_unmapped_rows = []
    if show_unmapped and unmapped_txns:
        for t in unmapped_txns:
            hidden_unmapped_rows.append(
                {
                    "trn_no": t.trn_no,
                    "date": t.date,
                    "payee": getattr(t, "payee", None),
                    "details": getattr(t, "details", None),
                    "amount": float(_csv_total_signed(t)),
                    "asset_csv_account": t.lines[0].csv_account_name if getattr(t, "lines", None) else None,
                }
            )
    all_accounts = get_accounts()
    mapping_rows = [{"csv_name": n, "account_id": mappings.get(n)} for n in sorted(mappings.keys())]

    return render_template(
        "transactions/import.html",
        csv_path=csv_path,
        start_date=start_date_str,
        rows=preview_rows,
        missing_mappings=missing_mappings,
        mappings=mappings,
        mapping_rows=mapping_rows,
        show_mappings=show_mappings,
        accounts=[a for a in all_accounts if a.type in ASSET_TYPES],
        all_accounts=all_accounts,
        txns_count=len(txns),
        preview_count=len(preview_rows),
        reviewed_duplicates=reviewed_duplicates,
        show_duplicates=show_duplicates,
        show_unmapped=show_unmapped,
        hidden_duplicate_rows=hidden_duplicate_rows,
        hidden_imported_rows=hidden_imported_rows,
        hidden_unmapped_rows=hidden_unmapped_rows,
        last_imported_ids=last_imported_ids or [],
    )
