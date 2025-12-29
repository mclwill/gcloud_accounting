# FlaskApp/app/services/balance_sheet_report.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date,timedelta
from typing import Any, Dict, List, Tuple

from sqlalchemy import case, func

from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.account import Account

# If you already have your P&L builder file, we can reuse it to compute Net Income.
from FlaskApp.app.services.pnl_report import build_pnl  # uses Income/Expense/COGS types


# --- Balance Sheet account types (from your CSV + common QB/Xero) ---
ASSET_TYPES = (
    "Bank",
    "Accounts Receivable",
    "Other Current Assets",
    "Fixed Assets",
    "Other Assets",
)

LIABILITY_TYPES = (
    "Accounts Payable",
    "Credit Card",
    "Other Current Liabilities",
    "Long Term Liabilities",
    "Other Liabilities",
)

EQUITY_TYPES = ("Equity",)

BS_TYPES = ASSET_TYPES + LIABILITY_TYPES + EQUITY_TYPES


# --- How types map into Balance Sheet sections + subgroups ---
SECTION_ORDER = ["Assets", "Liabilities", "Equity"]

TYPE_TO_SECTION = {t: "Assets" for t in ASSET_TYPES} | {t: "Liabilities" for t in LIABILITY_TYPES} | {t: "Equity" for t in EQUITY_TYPES}

# These are the “Current Assets / Current Liabilities” headings like your example.
TYPE_TO_GROUP = {
    # Assets
    "Bank": "Current Assets",
    "Accounts Receivable": "Current Assets",
    "Other Current Assets": "Current Assets",
    "Fixed Assets": "Fixed Assets",
    "Other Assets": "Other Assets",
    # Liabilities
    "Accounts Payable": "Current Liabilities",
    "Credit Card": "Current Liabilities",
    "Other Current Liabilities": "Current Liabilities",
    "Long Term Liabilities": "Long Term Liabilities",
    "Other Liabilities": "Long Term Liabilities",
    # Equity
    "Equity": "Shareholders' equity",
}

# Option: include a “Net Income” line under Equity using FY-to-date P&L
INCLUDE_NET_INCOME_LINE = True
FISCAL_YEAR_START_MONTH = 7  # July (matches your example)
FISCAL_YEAR_START_DAY = 1

def _parse_path(name: str) -> List[str]:
    return [p.strip() for p in name.split(":") if p.strip()]


def _bs_display_amount(account_type: str, net_activity: float) -> float:
    """
    net_activity is debit-positive / credit-negative (sum(debits) - sum(credits)).

    Display on Balance Sheet:
      - Assets typically debit balances -> show as-is
      - Liabilities/Equity typically credit balances -> flip sign so they show positive
    """
    if account_type in LIABILITY_TYPES or account_type in EQUITY_TYPES:
        return -float(net_activity or 0.0)
    return float(net_activity or 0.0)


@dataclass
class Node:
    name: str
    children: Dict[str, "Node"] = field(default_factory=dict)
    direct: List[float] = field(default_factory=list)  # per column
    total: List[float] = field(default_factory=list)   # per column

    def ensure_len(self, n: int) -> None:
        if len(self.direct) < n:
            self.direct.extend([0.0] * (n - len(self.direct)))
        if len(self.total) < n:
            self.total.extend([0.0] * (n - len(self.total)))


def _add_amount(root: Node, path: List[str], col_idx: int, amount: float, n_cols: int) -> None:
    cur = root
    for part in path:
        cur = cur.children.setdefault(part, Node(part))
    cur.ensure_len(n_cols)
    cur.direct[col_idx] += float(amount or 0.0)


def _rollup(node: Node, n_cols: int) -> List[float]:
    node.ensure_len(n_cols)
    total = node.direct[:]
    for child in node.children.values():
        child_total = _rollup(child, n_cols)
        total = [total[i] + child_total[i] for i in range(n_cols)]
    node.total = total
    return total


def _flatten(node: Node, base_path: List[str], level: int, n_cols: int) -> List[Dict[str, Any]]:
    """
    Flattens while storing full path (no ambiguity, no searching by label).
    Emits:
      - node line
      - child lines
      - Total node line if node has children
    """
    rows: List[Dict[str, Any]] = []

    path = base_path + [node.name]
    full_path = ":".join(path)
    has_children = bool(node.children)

    rows.append({
        "label": node.name,
        "path": path,
        "full_path": full_path,
        "level": level,
        "kind": "group" if has_children else "account",
        "cols": node.total[:n_cols],
        "row_total": sum(node.total[:n_cols]),
    })

    for key in sorted(node.children.keys()):
        rows.extend(_flatten(node.children[key], path, level + 1, n_cols))

    if has_children:
        rows.append({
            "label": f"Total {node.name}",
            "path": path,
            "full_path": full_path,
            "level": level,
            "kind": "total",
            "cols": node.total[:n_cols],
            "row_total": sum(node.total[:n_cols]),
        })

    return rows


def _query_asof_net_activity(entity_id: int, as_of: date) -> List[Tuple[str, str, float]]:
    """
    Returns (account_type, account_name, net_activity_asof)
    net_activity_asof = sum(debits) - sum(credits) for all txns <= as_of
    """
    net_expr = func.sum(
        case(
            (TransactionLine.is_debit.is_(True), TransactionLine.amount),
            else_=-TransactionLine.amount,
        )
    )

    q = (
        db.session.query(
            Account.type.label("account_type"),
            Account.name.label("account_name"),
            net_expr.label("net_activity"),
        )
        .join(TransactionLine, TransactionLine.account_id == Account.id)
        .join(Transaction, Transaction.id == TransactionLine.transaction_id)
        .filter(Account.entity_id == entity_id)
        .filter(Transaction.entity_id == entity_id)
        .filter(Account.type.in_(BS_TYPES))
        .filter(Transaction.date <= as_of)
        .group_by(Account.type, Account.name)
        .order_by(Account.type, Account.name)
    )

    return [(r.account_type, r.account_name, float(r.net_activity or 0.0)) for r in q.all()]

def _min_transaction_date(entity_id: int) -> date | None:
    return (
        db.session.query(func.min(Transaction.date))
        .filter(Transaction.entity_id == entity_id)
        .scalar()
    )


def _retained_earnings_distributions(entity_id: int, as_of: date) -> float:
    """
    Returns distributions posted to Retained Earnings accounts:
      debit-positive / credit-negative (debits - credits)
    In your model these are typically DEBITS (distributions), so this should be positive.
    """
    net_expr = func.sum(
        case(
            (TransactionLine.is_debit.is_(True), TransactionLine.amount),
            else_=-TransactionLine.amount,
        )
    )

    val = (
        db.session.query(net_expr)
        .select_from(Account)
        .join(TransactionLine, TransactionLine.account_id == Account.id)
        .join(Transaction, Transaction.id == TransactionLine.transaction_id)
        .filter(Account.entity_id == entity_id)
        .filter(Transaction.entity_id == entity_id)
        .filter(Account.type == "Equity")
        .filter(Account.name.ilike("%retained earnings%"))
        .filter(Transaction.date <= as_of)
        .scalar()
    )
    return float(val or 0.0)


def _net_income_for_range(entity_id: int, start: date, end: date) -> float:
    """
    Uses your P&L builder which returns net_profit as a *positive display* number.
    """
    pnl = build_pnl(entity_id, [("Period", start, end)])
    return float(pnl["totals"]["net_profit"][0] or 0.0)

def _pnl_net_income(entity_id: int, start: date, end: date) -> float:
    """
    Uses your P&L builder. It returns net_profit as a positive display number.
    """
    pnl = build_pnl(entity_id, [("Period", start, end)])
    return float(pnl["totals"]["net_profit"][0] or 0.0)

def _fy_start(d: date) -> date:
    # FY starts July 1. If month >= July => FY start is current year; else previous year.
    fy_year = d.year if d.month >= FISCAL_YEAR_START_MONTH else d.year - 1
    return date(fy_year, FISCAL_YEAR_START_MONTH, FISCAL_YEAR_START_DAY)


def _retained_earnings_distributions(entity_id: int, as_of: date) -> float:
    """
    Distributions are postings to Retained Earnings accounts.
    We calculate debit-positive / credit-negative activity through as_of.
    In your data, these are typically DEBITS, so expect positive values.
    """
    net_expr = func.sum(
        case(
            (TransactionLine.is_debit.is_(True), TransactionLine.amount),
            else_=-TransactionLine.amount,
        )
    )

    val = (
        db.session.query(net_expr)
        .select_from(Account)
        .join(TransactionLine, TransactionLine.account_id == Account.id)
        .join(Transaction, Transaction.id == TransactionLine.transaction_id)
        .filter(Account.entity_id == entity_id)
        .filter(Transaction.entity_id == entity_id)
        .filter(Account.type == "Equity")
        .filter(Account.name.ilike("%retained earnings%"))
        .filter(Transaction.date <= as_of)
        .scalar()
    )

    return float(val or 0.0)


def build_balance_sheet(entity_id: int, cols: list[tuple[str, date]]) -> dict:
    """
    cols = [(label, as_of_date), ...]
    Produces a Balance Sheet where:
      - Assets/Liabilities/Equity account balances are as-of
      - Retained Earnings is computed as (prior-years net income) - (distributions through as-of)
      - Net Income is current FY net income (FY start -> as-of)

    Distributions may occur in the current FY, so we subtract them through as_of.
    """
    if not cols:
        raise ValueError("cols must not be empty")

    n = len(cols)
    trees: dict[str, Node] = {sec: Node(sec) for sec in SECTION_ORDER}

    min_d = _min_transaction_date(entity_id)

    for col_idx, (_, as_of) in enumerate(cols):
        # 1) Load all BS accounts as-of and populate trees,
        #    EXCEPT Retained Earnings accounts (we treat those as distributions bucket).
        rows = _query_asof_net_activity(entity_id, as_of)

        for acct_type, acct_name, net in rows:
            section = TYPE_TO_SECTION.get(acct_type)
            if not section:
                continue

            # Skip any actual "Retained Earnings" equity accounts to avoid double-counting
            if acct_type == "Equity" and "retained earnings" in (acct_name or "").lower():
                continue

            display_amt = _bs_display_amount(acct_type, net)

            group = TYPE_TO_GROUP.get(acct_type, acct_type)
            acct_path = [group] + _parse_path(acct_name)

            _add_amount(trees[section], acct_path, col_idx, display_amt, n)

        # 2) Inject computed Retained Earnings + Net Income
        if min_d:
            fy_start = _fy_start(as_of)
            day_before_fy = fy_start - timedelta(days=1)

            prior_net_income = 0.0
            if day_before_fy >= min_d:
                prior_net_income = _pnl_net_income(entity_id, min_d, day_before_fy)

            current_net_income = _pnl_net_income(entity_id, fy_start, as_of)

            distributions = _retained_earnings_distributions(entity_id, as_of)

            retained_earnings = prior_net_income - distributions

            # Put both under Equity -> Shareholders' equity
            _add_amount(
                trees["Equity"],
                ["Shareholders' equity", "Retained Earnings"],
                col_idx,
                retained_earnings,
                n,
            )

            _add_amount(
                trees["Equity"],
                ["Shareholders' equity", "Net Income"],
                col_idx,
                current_net_income,
                n,
            )

    # Roll up and flatten
    out_sections: list[dict] = []
    section_totals: dict[str, list[float]] = {}

    for sec in SECTION_ORDER:
        _rollup(trees[sec], n)
        root = trees[sec]

        sec_rows: list[dict] = []
        for key in sorted(root.children.keys()):
            sec_rows.extend(_flatten(root.children[key], base_path=[], level=0, n_cols=n))

        section_totals[sec] = root.total[:n]
        out_sections.append({
            "section": sec,
            "rows": sec_rows,
            "section_totals": root.total[:n],
            "section_total": sum(root.total[:n]),
        })

    assets = section_totals.get("Assets", [0.0] * n)
    liabilities = section_totals.get("Liabilities", [0.0] * n)
    equity = section_totals.get("Equity", [0.0] * n)

    liab_plus_eq = [liabilities[i] + equity[i] for i in range(n)]
    difference = [assets[i] - liab_plus_eq[i] for i in range(n)]

    return {
        "entity_id": entity_id,
        "as_of": [{"label": lbl, "date": d.isoformat()} for (lbl, d) in cols],
        "sections": out_sections,
        "totals": {
            "assets": assets,
            "liabilities": liabilities,
            "equity": equity,
            "liabilities_plus_equity": liab_plus_eq,
            "difference": difference,
        },
    }