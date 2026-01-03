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

# ------------------------------
# CSV Import (Banktivity)
# ------------------------------

@bp.route("/import", methods=["GET", "POST"])
@login_required
def import_csv():
    """Preview Banktivity CSV, show matches, allow importing missing txns."""
    entity_id = _current_entity_id()

    show_mappings = (request.values.get("show_mappings") == "1")

    # Date filter
    start_date_str = request.values.get("start_date") or ""
    start_date: Optional[date] = None
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

    csv_path = request.values.get("csv_path") or ""

    # Recently imported transaction ids (stored in session on POST).
    last_imported_ids = session.pop("csv_last_imported_ids", None)

    if request.method == "POST" and "csv_file" in request.files and request.files["csv_file"].filename:
        # Save upload to a temp path under instance folder (works locally + in prod)
        f = request.files["csv_file"]
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        safe_name = f.filename.replace("/", "_").replace("\\", "_")
        tmp_dir = (bp.root_path + "/../tmp")
        import os
        os.makedirs(tmp_dir, exist_ok=True)
        csv_path = os.path.join(tmp_dir, f"banktivity_{entity_id}_{ts}_{safe_name}")
        f.save(csv_path)

    txns = []
    missing_mappings: List[str] = []
    mapping_rows: List[dict] = []
    existing_ids: set[int] = set()  # kept for backward-compat; not used for matching
    mappings: Dict[str, int] = {}

    # Load saved CSV->Asset account mappings for this entity
    for m in (
        db.session.query(CsvAccountMapping)
        .filter(CsvAccountMapping.entity_id == entity_id)
        .filter(CsvAccountMapping.source == "banktivity")
        .all()
    ):
        mappings[m.csv_account_name] = int(m.account_id)

    possible_matches: Dict[int, list[dict]] = {}

    if csv_path:
        txns = parse_banktivity_csv(csv_path, start_date=start_date)

        # Possible matches based on (date, amount)
        tol = 0.005
        for t in txns:
            amt = float(t.total_abs or 0)
            if amt <= 0:
                possible_matches[t.trn_no] = []
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
            possible_matches[t.trn_no] = [
                {"id": int(c.id), "description": (c.description or "")}
                for c in candidates
            ]

        # Enrich match candidates with the *other side* (non-asset) account name(s)
        candidate_ids = {m["id"] for lst in possible_matches.values() for m in lst}
        candidate_other: Dict[int, str] = {}
        if candidate_ids:
            rows = (
                db.session.query(TransactionLine.transaction_id, Account.name)
                .join(Account, Account.id == TransactionLine.account_id)
                .filter(TransactionLine.transaction_id.in_(candidate_ids))
                .filter(~Account.type.in_(ASSET_TYPES))
                .all()
            )
            tmp: Dict[int, set[str]] = {}
            for txn_id, acct_name in rows:
                tmp.setdefault(int(txn_id), set()).add(str(acct_name))

            # Fallback to asset account if a transaction has no non-asset lines
            if not tmp:
                tmp = {}

            asset_rows = (
                db.session.query(TransactionLine.transaction_id, Account.name)
                .join(Account, Account.id == TransactionLine.account_id)
                .filter(TransactionLine.transaction_id.in_(candidate_ids))
                .filter(Account.type.in_(ASSET_TYPES))
                .all()
            )
            asset_first: Dict[int, str] = {}
            for txn_id, acct_name in asset_rows:
                asset_first.setdefault(int(txn_id), str(acct_name))

            for txn_id in candidate_ids:
                names = sorted(tmp.get(int(txn_id), set()), key=lambda s: s.lower())
                candidate_other[int(txn_id)] = ", ".join(names) if names else asset_first.get(int(txn_id), "")

            for trn_no, lst in possible_matches.items():
                for m in lst:
                    m["asset_account"] = candidate_other.get(int(m["id"]), "")


        # Collect unmapped accounts present in the CSV
        csv_accounts = sorted({ln.csv_account_name for t in txns for ln in t.lines})
        missing_mappings = [a for a in csv_accounts if a not in mappings]

        # Build mapping rows for UI: show existing mappings plus any unmapped CSV accounts
        existing_names = set(mappings.keys())
        all_names = sorted(existing_names.union(set(missing_mappings)))
        mapping_rows = [{"csv_name": n, "account_id": mappings.get(n)} for n in all_names]

        if len(txns) == 0:
            flash("No transactions found for the selected start date. Try an earlier date.", "warning")

    if not csv_path:
        # When no CSV is loaded yet, still show any existing mappings.
        mapping_rows = [{"csv_name": n, "account_id": mappings.get(n)} for n in sorted(mappings.keys())]

    # Handle imports
    if request.method == "POST" and request.form.get("action") == "import_selected":
        # Selected transactions to import (may be empty if user is only marking duplicates)
        selected = request.form.getlist("import_trn_no")
        selected_ids = {int(x) for x in selected if x}

        # Duplicates marked by the user (persist regardless of whether anything is imported)
        dup_ids = {int(x) for x in request.form.getlist("dup_trn_no") if x}

        if not csv_path:
            flash("No CSV uploaded.", "error")
            return redirect(url_for("transactions_ui.import_csv"))

        # Re-parse to avoid trusting form fields (used for both dup marking and import)
        txns = parse_banktivity_csv(csv_path, start_date=start_date)
        txn_by_id = {t.trn_no: t for t in txns}

        # Refresh mappings
        mappings = {
            m.csv_account_name: int(m.account_id)
            for m in (
                db.session.query(CsvAccountMapping)
                .filter(CsvAccountMapping.entity_id == entity_id)
                .filter(CsvAccountMapping.source == "banktivity")
                .all()
            )
        }

        # Persist duplicate reviews first (independent of import)
        saved_dups = 0
        for trn_no in sorted(dup_ids):
            t = txn_by_id.get(trn_no)
            if not t or not getattr(t, "lines", None):
                continue
            asset_csv_acct = t.lines[0].csv_account_name
            asset_acct_id = mappings.get(asset_csv_acct)
            if not asset_acct_id:
                continue
            fp = _csv_fingerprint_for_txn(t, int(asset_acct_id))
            exists = (
                db.session.query(CsvImportReview.id)
                .filter(CsvImportReview.entity_id == entity_id)
                .filter(CsvImportReview.source == "banktivity")
                .filter(CsvImportReview.fingerprint == fp)
                .first()
                is not None
            )
            if not exists:
                db.session.add(
                    CsvImportReview(
                        entity_id=entity_id,
                        source="banktivity",
                        fingerprint=fp,
                        status="duplicate",
                        linked_transaction_id=None,
                        reviewed_at=datetime.utcnow(),
                    )
                )
                saved_dups += 1

        if saved_dups:
            db.session.commit()
            current_app.logger.info("CSV DUPLICATE SAVE: persisted %s duplicate(s)", saved_dups)

        # Never import rows marked as duplicates
        if dup_ids:
            selected_ids = {x for x in selected_ids if x not in dup_ids}

        if not selected_ids:
            if saved_dups:
                flash(f"Saved {saved_dups} duplicate review(s).", "success")
            else:
                flash("No transactions selected.", "warning")
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str, show_mappings="1"))

        # Refresh mappings
        mappings = {
            m.csv_account_name: int(m.account_id)
            for m in (
                db.session.query(CsvAccountMapping)
                .filter(CsvAccountMapping.entity_id == entity_id)
                .filter(CsvAccountMapping.source == "banktivity")
                .all()
            )
        }

        # Validate all required mappings exist for selected transactions
        unmapped_needed = set()
        for trn_no in selected_ids:
            t = txn_by_id.get(trn_no)
            if not t:
                continue
            for ln in t.lines:
                if ln.csv_account_name not in mappings:
                    unmapped_needed.add(ln.csv_account_name)

        if unmapped_needed:
            flash(
                "Cannot import yet. Please map these CSV accounts first: " + ", ".join(sorted(unmapped_needed)),
                "error",
            )
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

        imported = 0
        imported_txn_ids: List[int] = []

        # If a CSV transaction has possible DB matches, require an explicit confirmation checkbox.
        confirmed = {int(x) for x in request.form.getlist("confirm_trn_no") if x}

        # If user marked a row as a confirmed duplicate, remember it and do NOT import it.
        dup_ids = {int(x) for x in request.form.getlist("dup_trn_no") if x}

                # If user marked rows as duplicates, persist those reviews FIRST so they are remembered
        # even if we later rollback due to unconfirmed possible matches.
        saved_dups = 0
        if dup_ids:
            # Note: allow marking duplicates even if they were not selected for import.
            for trn_no in sorted(dup_ids):
                t = txn_by_id.get(trn_no)
                if not t or not t.lines:
                    continue

                asset_csv_acct = t.lines[0].csv_account_name
                asset_acct_id = mappings.get(asset_csv_acct)
                if not asset_acct_id:
                    # Can't fingerprint without asset mapping; skip saving
                    continue

                signed_amt = t.lines[0].amount if t.lines[0].is_debit else -t.lines[0].amount
                term = (t.payee or t.details or "").strip()
                fp = _csv_txn_fingerprint(t.date, signed_amt, int(asset_acct_id), term)

                exists = (
                    db.session.query(CsvImportReview.id)
                    .filter(CsvImportReview.entity_id == entity_id)
                    .filter(CsvImportReview.source == "banktivity")
                    .filter(CsvImportReview.fingerprint == fp)
                    .first()
                    is not None
                )
                if not exists:
                    db.session.add(
                        CsvImportReview(
                            entity_id=entity_id,
                            source="banktivity",
                            fingerprint=fp,
                            status="duplicate",
                            linked_transaction_id=None,
                            reviewed_at=datetime.utcnow(),
                        )
                    )
                    saved_dups += 1

            if saved_dups:
                db.session.commit()
                flash(f"Remembered {saved_dups} duplicate(s).", "success")

        # Ensure duplicates are never imported even if user also checked "Import?"
        selected_ids = {x for x in selected_ids if x not in dup_ids}


        tol = 0.005

        next_txn_id = (
            db.session.query(db.func.max(Transaction.transaction_id))
            .filter(Transaction.entity_id == entity_id)
            .scalar()
            or 0
        )

        blocked_for_review: list[int] = []

        for trn_no in sorted(selected_ids):
            t = txn_by_id.get(trn_no)
            if not t:
                continue

            # If marked as duplicate: store review + skip import
            if trn_no in dup_ids:
                ''' - change to ensure duplicates are remembered
                if t.lines:
                    asset_csv_acct = t.lines[0].csv_account_name
                    asset_acct_id = mappings.get(asset_csv_acct)
                    if asset_acct_id:
                        signed_amt = t.lines[0].amount if t.lines[0].is_debit else -t.lines[0].amount
                        term = t.payee or t.details or ""
                        fp = _csv_txn_fingerprint(t.date, signed_amt, int(asset_acct_id), term)

                        exists = (
                            db.session.query(CsvImportReview.id)
                            .filter(CsvImportReview.entity_id == entity_id)
                            .filter(CsvImportReview.source == "banktivity")
                            .filter(CsvImportReview.fingerprint == fp)
                            .first()
                            is not None
                        )
                        if not exists:
                            db.session.add(
                                CsvImportReview(
                                    entity_id=entity_id,
                                    source="banktivity",
                                    fingerprint=fp,
                                    status="duplicate",
                                    linked_transaction_id=None,
                                    reviewed_at=datetime.utcnow(),
                                )
                            )
                '''
                continue

            # Optional counter-account from dropdown
            counter_account_id = None
            counter_val = request.form.get(f"counter__{trn_no}") or ""
            if counter_val.strip():
                try:
                    counter_account_id = int(counter_val)
                except ValueError:
                    counter_account_id = None

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

            # Remember imported fingerprint
            if t.lines:
                asset_csv_acct = t.lines[0].csv_account_name
                asset_acct_id = mappings.get(asset_csv_acct)
                if asset_acct_id:
                    signed_amt = t.lines[0].amount if t.lines[0].is_debit else -t.lines[0].amount
                    term = t.payee or t.details or ""
                    fp = _csv_txn_fingerprint(t.date, signed_amt, int(asset_acct_id), term)

                    exists = (
                        db.session.query(CsvImportReview.id)
                        .filter(CsvImportReview.entity_id == entity_id)
                        .filter(CsvImportReview.source == "banktivity")
                        .filter(CsvImportReview.fingerprint == fp)
                        .first()
                        is not None
                    )
                    if not exists:
                        db.session.add(
                            CsvImportReview(
                                entity_id=entity_id,
                                source="banktivity",
                                fingerprint=fp,
                                status="imported",
                                linked_transaction_id=txn.id,
                                reviewed_at=datetime.utcnow(),
                            )
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

            imported += 1

        if blocked_for_review:
            db.session.rollback()
            flash(
                "Some selected rows have possible date/amount matches in the database and were not confirmed. "
                f"Please review and tick 'Confirm' for: {', '.join(str(x) for x in blocked_for_review)}",
                "error",
            )
            return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

        db.session.commit()
        session["csv_last_imported_ids"] = imported_txn_ids
        flash(f"Imported {imported} transaction(s).", "success")
        return redirect(url_for("transactions_ui.import_csv", csv_path=csv_path, start_date=start_date_str))

    # Handle mapping saves
    if request.method == "POST" and request.form.get("action") == "save_mappings":
        # Expect form fields:
        #  - csv_names (repeated hidden inputs with the CSV account names)
        #  - map_to__<index> = <account_id>
        updates = 0
        csv_names = request.form.getlist("csv_names")
        for idx, csv_name in enumerate(csv_names):
            csv_name = (csv_name or "").strip()
            if not csv_name:
                continue
            v = request.form.get(f"map_to__{idx}")

            existing = (
                db.session.query(CsvAccountMapping)
                .filter(CsvAccountMapping.entity_id == entity_id)
                .filter(CsvAccountMapping.source == "banktivity")
                .filter(CsvAccountMapping.csv_account_name == csv_name)
                .one_or_none()
            )

            # User cleared the mapping → delete existing row
            if not v:
                if existing:
                    db.session.delete(existing)
                    updates += 1
                continue

            try:
                acct_id = int(v)
            except Exception:
                continue
                
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

    # Render preview
    # Only show transactions whose *asset* CSV account has been mapped for this entity.
    preview_txns = []
    unmapped_accounts = set()

    for t in txns:
        if not t.lines:
            continue
        csv_acct = t.lines[0].csv_account_name
        if csv_acct in mappings:
            preview_txns.append(t)
        else:
            unmapped_accounts.add(csv_acct)

    missing_mappings = sorted(unmapped_accounts)

    # Hide CSV rows previously reviewed as duplicates for this entity/source.
    reviewed_duplicates = []
    if preview_txns:
        fp_by_trn = {}
        fps = []
        for t in preview_txns:
            csv_acct = t.lines[0].csv_account_name if t.lines else ""
            asset_id = mappings.get(csv_acct)
            if not asset_id:
                continue
            signed_amt = t.lines[0].amount if t.lines[0].is_debit else -t.lines[0].amount
            term = t.payee or t.details or ""
            fp = _csv_txn_fingerprint(t.date, signed_amt, int(asset_id), term)
            fp_by_trn[t.trn_no] = fp
            fps.append(fp)

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
            if r and r.status == "duplicate":
                reviewed_duplicates.append({"trn_no": t.trn_no, "linked_transaction_id": r.linked_transaction_id})
                continue
            filtered.append(t)
        preview_txns = filtered

    # Suggest a likely non-asset counterparty account based on historical transactions.
    # Heuristic: find the most common non-asset account used on transactions whose
    # description contains the CSV payee (or details).
    suggestions: Dict[int, Optional[dict]] = {}
    if csv_path and preview_txns:
        term_for_trn: Dict[int, str] = {}
        unique_terms: Dict[str, Optional[dict]] = {}

        for t in preview_txns:
            term = (t.payee or t.details or "").strip()
            if not term:
                continue
            # keep terms short-ish to avoid wild LIKE matches
            term = term[:64]
            term_for_trn[t.trn_no] = term
            unique_terms.setdefault(term, None)

        # Subquery of non-asset accounts (used for suggestions). Use an explicit
        # SELECT in the IN() clause to avoid SAWarnings about coercing subqueries.
        non_asset_accounts_sq = (
            db.session.query(Account.id)
            .filter(Account.entity_id == entity_id)
            .filter(~Account.type.in_(ASSET_TYPES))
            .subquery()
        )

        for term in unique_terms.keys():
            # Find most common non-asset account used on similar descriptions.
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

    preview_rows = []
    for t in preview_txns:
        candidates = possible_matches.get(t.trn_no, [])
        csv_acct = t.lines[0].csv_account_name if t.lines else ""
        # Backward-compatible keys expected by the template.
        match_count = len(candidates)
        preview_rows.append(
            {
                "trn_no": t.trn_no,
                "date": t.date,
                "payee": t.payee,
                "type": t.type,
                "details": t.details,
                "total": t.total_abs,
                "csv_account": csv_acct,
                "mapped_account_id": mappings.get(csv_acct),
                "mapped_account_name": (account_by_id.get(mappings.get(csv_acct)).name if mappings.get(csv_acct) and account_by_id.get(mappings.get(csv_acct)) else ""),
                "possible_matches": candidates,
                "needs_confirm": bool(candidates),
                "match_count": match_count,
                "match_candidates": candidates,
                "suggested": suggestions.get(t.trn_no),
                "lines": t.lines,
            }
        )
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
        last_imported_ids=last_imported_ids or [],
    )
