from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from flask import Blueprint, abort, flash, redirect, render_template, request, session, url_for, current_app
from flask_login import login_required
from sqlalchemy import func, select
from sqlalchemy.orm import load_only

from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.account import Account
from FlaskApp.app.models.csv_account_mapping import CsvAccountMapping
from FlaskApp.app.models.csv_import_review import CsvImportReview
from FlaskApp.app.models.entity import Entity
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.services.accounts import get_accounts
from FlaskApp.app.services.balance_sheet_report import ASSET_TYPES
from FlaskApp.app.services.banktivity_import import parse_banktivity_csv
from FlaskApp.app.services.entities import get_entities
from FlaskApp.app.services.transaction_detail import get_transaction_detail
from FlaskApp.app.services.transaction_list import get_transaction_list

bp = Blueprint("transactions_ui", __name__, url_prefix="/transactions")


# ------------------------------
# Helpers
# ------------------------------
def _current_entity_id() -> int:
    name = session.get("current_entity") or "JAJG Pty Ltd"
    session["current_entity"] = name
    ent = db.session.query(Entity).filter(Entity.name == name).one_or_none()
    if not ent:
        abort(400, description=f"Unknown entity '{name}'")
    return int(ent.id)


def _csv_txn_fingerprint(dt: date, signed_amount: float, asset_account_id: int, term: str) -> str:
    """
    Stable fingerprint for remembering reviewed CSV rows.

    IMPORTANT: Any code that persists or looks up CsvImportReview must use the same
    normalization, otherwise duplicates won't be found/hidden.
    """
    import hashlib

    amt = round(float(signed_amount), 2)
    norm_term = (term or "").strip().lower()[:64]
    raw = f"{dt.isoformat()}|{amt:.2f}|{int(asset_account_id)}|{norm_term}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _csv_fingerprint_for_txn(t, asset_account_id: int) -> str:
    if not getattr(t, "lines", None):
        raise ValueError("CSV txn has no lines")
    ln = t.lines[0]
    signed_amt = float(ln.amount) if ln.is_debit else -float(ln.amount)
    term = (t.payee or t.details or "")
    return _csv_txn_fingerprint(t.date, signed_amt, int(asset_account_id), term)


def _parse_dup_ids_from_form() -> set[int]:
    """
    Template posts dup checkboxes as:
      name="dup_trn_no__<TRN_NO>" value="1"
    """
    ids: set[int] = set()
    for k, v in request.form.items():
        if not k.startswith("dup_trn_no__"):
            continue
        if not (v or "").strip():
            continue
        try:
            ids.add(int(k.split("__", 1)[1]))
        except Exception:
            continue
    return ids


def _upsert_review(
    *,
    entity_id: int,
    fingerprint: str,
    status: str,
    linked_transaction_id: Optional[int] = None,
) -> None:
    """
    Ensure a CsvImportReview row exists and is updated.

    We do an upsert because users can:
      - mark duplicates repeatedly
      - later import a row that was previously marked duplicate (after "unhide")
    """
    row = (
        db.session.query(CsvImportReview)
        .filter(CsvImportReview.entity_id == entity_id)
        .filter(CsvImportReview.source == "banktivity")
        .filter(CsvImportReview.fingerprint == fingerprint)
        .one_or_none()
    )
    if row is None:
        row = CsvImportReview(
            entity_id=entity_id,
            source="banktivity",
            fingerprint=fingerprint,
        )
        db.session.add(row)

    row.status = status
    row.linked_transaction_id = linked_transaction_id
    row.reviewed_at = datetime.utcnow()


# ------------------------------
# Transactions list + detail
# ------------------------------
@bp.route("/", endpoint="list")
@login_required
def index():
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
    return render_template("transactions/detail.html", transaction=transaction, lines=lines)


# ------------------------------
# CSV Import (Banktivity)
# ------------------------------
@bp.route("/import", methods=["GET", "POST"])
@login_required
def import_csv():
    entity_id = _current_entity_id()

    show_mappings = request.values.get("show_mappings") == "1"
    show_hidden = (request.values.get("show_hidden") == "1")

    start_date_str = request.values.get("start_date") or ""
    start_date: Optional[date] = None
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

    csv_path = request.values.get("csv_path") or ""
    last_imported_ids = session.pop("csv_last_imported_ids", None)

    # Upload handling
    if request.method == "POST" and "csv_file" in request.files and request.files["csv_file"].filename:
        f = request.files["csv_file"]
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        safe_name = f.filename.replace("/", "_").replace("\\", "_")
        import os

        tmp_dir = os.path.join(bp.root_path, "..", "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        csv_path = os.path.join(tmp_dir, f"banktivity_{entity_id}_{ts}_{safe_name}")
        f.save(csv_path)

    # Load persisted mappings (CSV asset account name -> account_id)
    mappings: Dict[str, int] = {
        m.csv_account_name: int(m.account_id)
        for m in (
            db.session.query(CsvAccountMapping)
            .filter(CsvAccountMapping.entity_id == entity_id)
            .filter(CsvAccountMapping.source == "banktivity")
            .all()
        )
    }

    # Parse csv (if provided)
    txns = parse_banktivity_csv(csv_path, start_date=start_date) if csv_path else []

    # Mapping rows (show existing + any new accounts from this CSV)
    csv_account_names = sorted({ln.csv_account_name for t in txns for ln in getattr(t, "lines", [])}) if txns else []
    all_map_names = sorted(set(mappings.keys()).union(csv_account_names))
    mapping_rows = [{"csv_name": n, "account_id": mappings.get(n)} for n in all_map_names]

    # ------------------------------
    # Save mappings
    # ------------------------------
    if request.method == "POST" and request.form.get("action") == "save_mappings":
        updates = 0
        csv_names = request.form.getlist("csv_names")
        seen: set[str] = set()

        with db.session.no_autoflush:
            for idx, csv_name in enumerate(csv_names):
                csv_name = (csv_name or "").strip()
                if not csv_name or csv_name in seen:
                    continue
                seen.add(csv_name)

                v = (request.form.get(f"map_to__{idx}") or "").strip()
                existing = (
                    db.session.query(CsvAccountMapping)
                    .filter(CsvAccountMapping.entity_id == entity_id)
                    .filter(CsvAccountMapping.source == "banktivity")
                    .filter(CsvAccountMapping.csv_account_name == csv_name)
                    .one_or_none()
                )

                # Clear mapping -> delete
                if not v:
                    if existing is not None:
                        db.session.delete(existing)
                        updates += 1
                    continue

                try:
                    acct_id = int(v)
                except ValueError:
                    continue

                if existing is not None:
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
        flash(f"Saved {updates} mapping change(s).", "success")
        return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str, show_mappings="1"))

    # ------------------------------
    # Import selected / mark duplicates
    # ------------------------------
    if request.method == "POST" and request.form.get("action") == "import_selected":
        selected_ids = {int(x) for x in request.form.getlist("import_trn_no") if x}
        dup_ids = _parse_dup_ids_from_form()
        confirmed = {int(x) for x in request.form.getlist("confirm_trn_no") if x}

        current_app.logger.warning(
            "CSV import POST: csv_path=%s start_date=%s selected_ids=%s dup_ids=%s total_csv_txns=%s",
            csv_path,
            start_date_str,
            sorted(selected_ids),
            sorted(dup_ids),
            len(txns),
        )

        if not csv_path:
            flash("No CSV uploaded.", "error")
            return redirect(url_for("transactions_ui.import_csv"))

        # Always re-parse and re-load mappings (don't trust posted values)
        txns = parse_banktivity_csv(csv_path, start_date=start_date)
        txn_by_id = {int(t.trn_no): t for t in txns}

        mappings = {
            m.csv_account_name: int(m.account_id)
            for m in (
                db.session.query(CsvAccountMapping)
                .filter(CsvAccountMapping.entity_id == entity_id)
                .filter(CsvAccountMapping.source == "banktivity")
                .all()
            )
        }

        # Persist duplicate markings, even if user isn't importing anything.
        saved_dups = 0
        for trn_no in sorted(dup_ids):
            t = txn_by_id.get(trn_no)
            if not t or not getattr(t, "lines", None):
                continue
            asset_csv_acct = t.lines[0].csv_account_name
            asset_acct_id = mappings.get(asset_csv_acct)
            if not asset_acct_id:
                # can't fingerprint without mapping
                continue

            fp = _csv_fingerprint_for_txn(t, int(asset_acct_id))
            current_app.logger.warning(
                "Remembering duplicate request: trn_no=%s fp=%s asset_account_id=%s",
                trn_no,
                fp,
                asset_acct_id,
            )
            _upsert_review(entity_id=entity_id, fingerprint=fp, status="duplicate", linked_transaction_id=None)
            saved_dups += 1

        if saved_dups:
            db.session.commit()
            current_app.logger.warning("CSV DUPLICATE SAVE: persisted %s duplicate(s)", saved_dups)

        # Never import rows marked duplicate
        if dup_ids:
            selected_ids = {x for x in selected_ids if x not in dup_ids}

        if not selected_ids:
            if saved_dups:
                flash(f"Saved {saved_dups} duplicate review(s).", "success")
            else:
                flash("No transactions selected.", "warning")
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str, show_mappings="1"))

        # Validate mappings exist for every line of every selected txn
        unmapped_needed: set[str] = set()
        for trn_no in selected_ids:
            t = txn_by_id.get(trn_no)
            if not t:
                continue
            for ln in getattr(t, "lines", []) or []:
                if ln.csv_account_name not in mappings:
                    unmapped_needed.add(ln.csv_account_name)

        if unmapped_needed:
            flash(
                "Cannot import yet. Please map these CSV accounts first: " + ", ".join(sorted(unmapped_needed)),
                "error",
            )
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str, show_mappings="1"))

        # Compute next transaction_id sequence number
        next_txn_id = (
            db.session.query(func.max(Transaction.transaction_id))
            .filter(Transaction.entity_id == entity_id)
            .scalar()
            or 0
        )

        tol = 0.005
        imported = 0
        imported_txn_ids: List[int] = []
        blocked_for_review: List[int] = []

        for trn_no in sorted(selected_ids):
            t = txn_by_id.get(trn_no)
            if not t:
                continue

            # Require confirm if candidates exist
            amt = float(t.total_abs or 0)
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

            next_txn_id += 1
            txn = Transaction(
                entity_id=entity_id,
                transaction_id=int(next_txn_id),
                date=t.date,
                description=t.payee or t.details or "",
                transaction_type=t.type,
                posted_at=datetime.combine(t.date, datetime.min.time()),
            )
            db.session.add(txn)
            db.session.flush()
            imported_txn_ids.append(int(txn.id))

            # Create mapped lines
            for ln in getattr(t, "lines", []) or []:
                acct_id = mappings[ln.csv_account_name]
                db.session.add(
                    TransactionLine(
                        transaction_id=txn.id,
                        account_id=acct_id,
                        is_debit=ln.is_debit,
                        amount=float(ln.amount),
                        memo=getattr(ln, "memo", None),
                    )
                )

            # Optional counter line (balanced) from dropdown
            counter_account_id = None
            counter_val = (request.form.get(f"counter__{trn_no}") or "").strip()
            if counter_val:
                try:
                    counter_account_id = int(counter_val)
                except ValueError:
                    counter_account_id = None

            if counter_account_id and getattr(t, "lines", None):
                asset_csv_acct = t.lines[0].csv_account_name
                asset_acct_id = mappings.get(asset_csv_acct)
                if asset_acct_id and counter_account_id == int(asset_acct_id):
                    flash(f"Counter account cannot be the same as the mapped asset account for TRN_NO {trn_no}.", "error")
                else:
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

            # Remember imported fingerprint
            if getattr(t, "lines", None):
                asset_csv_acct = t.lines[0].csv_account_name
                asset_acct_id = mappings.get(asset_csv_acct)
                if asset_acct_id:
                    fp = _csv_fingerprint_for_txn(t, int(asset_acct_id))
                    _upsert_review(
                        entity_id=entity_id,
                        fingerprint=fp,
                        status="imported",
                        linked_transaction_id=int(txn.id),
                    )

            imported += 1

        if blocked_for_review:
            db.session.rollback()
            flash(
                "Some selected rows have possible date/amount matches in the database and were not confirmed. "
                f"Please review and tick 'Confirm' for: {', '.join(str(x) for x in blocked_for_review)}",
                "error",
            )
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str, show_mappings="1"))

        db.session.commit()
        session["csv_last_imported_ids"] = imported_txn_ids
        flash(f"Imported {imported} transaction(s).", "success")
        return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str, show_mappings="1"))

    # ------------------------------
    # Build preview list (only mapped asset accounts)
    # ------------------------------
    preview_txns: List = []
    unmapped_txns: List = []
    unmapped_accounts: set[str] = set()

    for t in txns:
        if not getattr(t, "lines", None):
            continue
        csv_acct = t.lines[0].csv_account_name
        if csv_acct in mappings and mappings.get(csv_acct):
            preview_txns.append(t)
        else:
            unmapped_txns.append(t)
            unmapped_accounts.add(csv_acct)

    missing_mappings = sorted(unmapped_accounts)

    # ------------------------------
    # Hide previously-reviewed duplicates (by fingerprint)
    # ------------------------------
    reviewed_duplicates: List[dict] = []
    hidden_status_by_trn: Dict[int, dict] = {}
    if preview_txns:
        fp_by_trn: Dict[int, str] = {}
        fps: List[str] = []

        for t in preview_txns:
            csv_acct = t.lines[0].csv_account_name
            asset_id = mappings.get(csv_acct)
            if not asset_id:
                continue
            fp = _csv_fingerprint_for_txn(t, int(asset_id))
            fp_by_trn[int(t.trn_no)] = fp
            fps.append(fp)

        reviewed_by_fp: Dict[str, CsvImportReview] = {}
        if fps:
            reviewed_by_fp = {
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
            fp = fp_by_trn.get(int(t.trn_no))
            r = reviewed_by_fp.get(fp) if fp else None
            if r and r.status in {"duplicate", "imported"}:
                reviewed_duplicates.append({"trn_no": int(t.trn_no), "linked_transaction_id": r.linked_transaction_id})
                hidden_status_by_trn[int(t.trn_no)] = {
                    "status": r.status,
                    "linked_transaction_id": r.linked_transaction_id,
                }
                continue
            filtered.append(t)
        preview_txns = filtered



    # ------------------------------
    # Possible matches (date + amount)
    # ------------------------------
    possible_matches: Dict[int, List[dict]] = {}
    if txns:
        tol = 0.005
        for t in txns:
            amt = float(t.total_abs or 0)
            if amt <= 0:
                possible_matches[int(t.trn_no)] = []
                continue

            candidates = (
                db.session.query(Transaction)
                .options(load_only(Transaction.id, Transaction.description, Transaction.date))
                .join(TransactionLine)
                .filter(Transaction.entity_id == entity_id)
                .filter(Transaction.date == t.date)
                .filter(TransactionLine.amount.between(amt - tol, amt + tol))
                .distinct()
                .all()
            )
            possible_matches[int(t.trn_no)] = [{"id": int(c.id), "description": (c.description or "")} for c in candidates]

    # Enrich match candidates with "other side" account names.
    # Template expects `asset_account` field, but we show the counterparty account(s) instead.
    candidate_ids = {m["id"] for lst in possible_matches.values() for m in lst}
    txn_lines: Dict[int, List[Tuple[int, str, str]]] = {}  # txn_id -> [(acct_id, acct_type, acct_name)]
    if candidate_ids:
        for txn_id, acct_id, acct_type, acct_name in (
            db.session.query(
                TransactionLine.transaction_id,
                Account.id,
                Account.type,
                Account.name,
            )
            .join(Account, Account.id == TransactionLine.account_id)
            .filter(TransactionLine.transaction_id.in_(candidate_ids))
            .all()
        ):
            txn_lines.setdefault(int(txn_id), []).append((int(acct_id), str(acct_type), str(acct_name)))

    for t in preview_txns:
        csv_acct = t.lines[0].csv_account_name if getattr(t, "lines", None) else ""
        exclude_acct_id = mappings.get(csv_acct)  # asset account shown in the "Account" column

        for m in possible_matches.get(int(t.trn_no), []):
            tid = int(m["id"])
            lines = txn_lines.get(tid, [])

            # Prefer non-asset accounts (expense/income/liability/equity), excluding the mapped asset account.
            non_asset = [
                name
                for (aid, atype, name) in lines
                if (exclude_acct_id is None or aid != int(exclude_acct_id)) and atype not in ASSET_TYPES
            ]

            # If it's a transfer between assets, fall back to "other" accounts excluding mapped asset.
            if non_asset:
                names = sorted(set(non_asset), key=lambda s: s.lower())
            else:
                other = [name for (aid, _atype, name) in lines if (exclude_acct_id is None or aid != int(exclude_acct_id))]
                names = sorted(set(other), key=lambda s: s.lower())

            m["asset_account"] = ", ".join(names)

    # ------------------------------
    # Auto-hide duplicates created by this import
    # If a remaining CSV row matches a transaction we just imported (same date+amount),
    # remember it as a duplicate linked to that imported transaction and hide it immediately.
    # ------------------------------
    auto_marked = 0
    if last_imported_ids and preview_txns:
        imported_set = {int(x) for x in (last_imported_ids or [])}
        kept = []
        for t in preview_txns:
            trn_no = int(t.trn_no)
            hit = None
            for m in possible_matches.get(trn_no, []):
                if int(m.get("id", 0)) in imported_set:
                    hit = m
                    break
            if not hit:
                kept.append(t)
                continue

            # Persist as remembered duplicate (fingerprint-based) and hide from preview.
            try:
                csv_acct = t.lines[0].csv_account_name if getattr(t, "lines", None) else None
                asset_id = mappings.get(csv_acct) if csv_acct else None
                if asset_id:
                    fp = _csv_fingerprint_for_txn(t, int(asset_id))
                    _upsert_csv_review(
                        entity_id=entity_id,
                        source="banktivity",
                        fingerprint=fp,
                        status="duplicate",
                        linked_transaction_id=int(hit["id"]),
                    )
                    auto_marked += 1
                    current_app.logger.warning(
                        "Auto-marked duplicate due to just-imported match: trn_no=%s fp=%s linked_transaction_id=%s",
                        trn_no,
                        fp,
                        int(hit["id"]),
                    )
                else:
                    # If we can't fingerprint it (no mapping), keep it visible.
                    kept.append(t)
            except Exception as e:
                current_app.logger.exception("Auto-mark duplicate failed for trn_no=%s: %s", trn_no, e)
                kept.append(t)

        if auto_marked:
            db.session.commit()
            flash(f"Auto-hidden {auto_marked} duplicate(s) that match transactions you just imported.", "success")
        preview_txns = kept


    # ------------------------------
    # Suggest likely counterparty account (historical heuristic)
    # ------------------------------
    suggestions: Dict[int, Optional[dict]] = {}
    if csv_path and preview_txns:
        term_for_trn: Dict[int, str] = {}
        unique_terms: Dict[str, Optional[dict]] = {}

        for t in preview_txns:
            term = (t.payee or t.details or "").strip()
            if not term:
                continue
            term = term[:64]
            term_for_trn[int(t.trn_no)] = term
            unique_terms.setdefault(term, None)

        non_asset_accounts_sq = (
            db.session.query(Account.id)
            .filter(Account.entity_id == entity_id)
            .filter(~Account.type.in_(ASSET_TYPES))
            .subquery()
        )

        for term in tuple(unique_terms.keys()):
            res = (
                db.session.query(
                    Account.id.label("id"),
                    Account.name.label("name"),
                    func.count(TransactionLine.id).label("cnt"),
                )
                .select_from(Transaction)
                .join(TransactionLine, TransactionLine.transaction_id == Transaction.id)
                .join(Account, Account.id == TransactionLine.account_id)
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

    # Accounts for display/dropdowns
    all_accounts = get_accounts()
    account_by_id = {int(a.id): a for a in all_accounts}

    preview_rows: List[dict] = []
    for t in preview_txns:
        candidates = possible_matches.get(int(t.trn_no), [])
        csv_acct = t.lines[0].csv_account_name if getattr(t, "lines", None) else ""
        mapped_id = mappings.get(csv_acct)
        mapped_name = account_by_id.get(mapped_id).name if mapped_id and account_by_id.get(mapped_id) else ""

        preview_rows.append(
            {
                "trn_no": int(t.trn_no),
                "date": t.date,
                "payee": t.payee,
                "type": t.type,
                "details": t.details,
                "total": t.total_abs,
                "csv_account": csv_acct,
                "mapped_account_id": mapped_id,
                "mapped_account_name": mapped_name,
                "possible_matches": candidates,
                "needs_confirm": bool(candidates),
                "match_count": len(candidates),
                "match_candidates": candidates,
                "suggested": suggestions.get(int(t.trn_no)),
                "lines": getattr(t, "lines", None),
            }
        )


    # Build "hidden" rows for optional viewing
    hidden_review_rows: List[dict] = []
    if hidden_status_by_trn:
        for t in txns:
            trn = int(getattr(t, "trn_no", 0) or 0)
            meta = hidden_status_by_trn.get(trn)
            if not meta:
                continue
            csv_acct = t.lines[0].csv_account_name if getattr(t, "lines", None) else ""
            mapped_id = int(mappings.get(csv_acct) or 0)
            mapped_name = account_by_id.get(mapped_id).name if mapped_id and account_by_id.get(mapped_id) else ""
            # Some older duplicate-review rows were saved without a linked txn id.
            # If it's missing, try to infer it from the current match candidates.
            linked_id = meta.get("linked_transaction_id")
            if not linked_id:
                candidates = possible_matches.get(trn, [])
                if candidates:
                    linked_id = candidates[0].get("id")

            hidden_review_rows.append({
                "trn_no": trn,
                "date": t.date,
                "payee": getattr(t, "payee", "") or getattr(t, "details", "") or "",
                "type": getattr(t, "type", "") or "",
                "csv_account_name": csv_acct,
                "mapped_account_id": mapped_id or None,
                "mapped_account_name": mapped_name or csv_acct,
                "lines": getattr(t, "lines", None),
                "status": meta.get("status"),
                "linked_transaction_id": linked_id,
            })

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
        show_hidden=show_hidden,
        hidden_review_rows=hidden_review_rows,

        last_imported_ids=last_imported_ids or [],
    )