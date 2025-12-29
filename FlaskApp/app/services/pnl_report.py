# FlaskApp/app/services/pnl_report.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Tuple

from sqlalchemy import case, func

from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.transaction import Transaction
from FlaskApp.app.models.transaction_line import TransactionLine
from FlaskApp.app.models.account import Account


# ---- Account types from your CSV ----
PNL_TYPES = (
    "Income",
    "Other Income",
    "Cost of Goods Sold",
    "Expenses",
    "Other Expense",
)

# Top-level P&L sections
SECTION_ORDER = ["Income", "Cost of Goods Sold", "Expenses"]

# Map account types -> top-level section
TYPE_TO_SECTION = {
    "Income": "Income",
    "Other Income": "Income",
    "Cost of Goods Sold": "Cost of Goods Sold",
    "Expenses": "Expenses",
    "Other Expense": "Expenses",
}

# If True, show "Income"/"Other Income" and "Expenses"/"Other Expense" as subheadings
SHOW_TYPE_SUBHEADINGS = True


def _parse_path(name: str) -> List[str]:
    return [p.strip() for p in name.split(":") if p.strip()]


def _pnl_display_amount(account_type: str, net_activity: float) -> float:
    """
    net_activity = sum(debits) - sum(credits) for the period (debit +, credit -).
    Display convention:
      - Income & Other Income shown positive -> flip sign
      - Expenses / Other Expense / COGS shown positive -> keep sign
    """
    if account_type in ("Income", "Other Income"):
        return -float(net_activity or 0.0)
    return float(net_activity or 0.0)


@dataclass
class Node:
    name: str
    children: Dict[str, "Node"] = field(default_factory=dict)

    # per-period postings directly to this node
    direct: List[float] = field(default_factory=list)

    # per-period totals including descendants
    total: List[float] = field(default_factory=list)

    def ensure_len(self, n: int) -> None:
        if len(self.direct) < n:
            self.direct.extend([0.0] * (n - len(self.direct)))
        if len(self.total) < n:
            self.total.extend([0.0] * (n - len(self.total)))


def _add_amount(root: Node, path: List[str], period_idx: int, amount: float, n_periods: int) -> None:
    cur = root
    for part in path:
        cur = cur.children.setdefault(part, Node(part))
    cur.ensure_len(n_periods)
    cur.direct[period_idx] += float(amount or 0.0)


def _rollup(node: Node, n_periods: int) -> List[float]:
    node.ensure_len(n_periods)

    # start from direct postings
    total = node.direct[:]

    # add children totals
    for child in node.children.values():
        child_total = _rollup(child, n_periods)
        total = [total[i] + child_total[i] for i in range(n_periods)]

    node.total = total
    return total


def _flatten(node: Node, base_path: List[str], level: int, n_periods: int) -> List[Dict[str, Any]]:
    """
    Flattens preserving FULL PATH (no ambiguity if duplicate names exist elsewhere).
    We produce:
      - account line (node itself)
      - children lines
      - "Total X" line if node has children
    """
    rows: List[Dict[str, Any]] = []

    path = base_path + [node.name]
    full_path = ":".join(path)

    has_children = len(node.children) > 0

    rows.append({
        "label": node.name,
        "path": path,
        "full_path": full_path,
        "level": level,
        "kind": "group" if has_children else "account",
        "periods": node.total[:n_periods],
        "row_total": sum(node.total[:n_periods]),
    })

    for key in sorted(node.children.keys()):
        rows.extend(_flatten(node.children[key], path, level + 1, n_periods))

    if has_children:
        rows.append({
            "label": f"Total {node.name}",
            "path": path,                 # total belongs to the same node path
            "full_path": full_path,
            "level": level,
            "kind": "total",
            "periods": node.total[:n_periods],
            "row_total": sum(node.total[:n_periods]),
        })

    return rows


def _query_period_net_activity(entity_id: int, start_date: date, end_date: date) -> List[Tuple[str, str, float]]:
    """
    Returns (account_type, account_full_name, net_activity)
    net_activity uses debit-positive / credit-negative over the period.
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
        .filter(Account.type.in_(PNL_TYPES))
        .filter(Transaction.date >= start_date, Transaction.date <= end_date)
        .group_by(Account.type, Account.name)
        .order_by(Account.type, Account.name)
    )

    return [(r.account_type, r.account_name, float(r.net_activity or 0.0)) for r in q.all()]


def build_pnl(entity_id: int, periods: List[Tuple[str, date, date]]) -> Dict[str, Any]:
    """
    periods: [(label, start_date, end_date), ...]
    Returns JSON-friendly structure ready for Dash or Excel export.
    """
    if not periods:
        raise ValueError("periods must not be empty")

    n = len(periods)

    # One tree per top-level section
    trees: Dict[str, Node] = {sec: Node(sec) for sec in SECTION_ORDER}

    # Fill trees with period amounts
    for p_idx, (_, start, end) in enumerate(periods):
        rows = _query_period_net_activity(entity_id, start, end)
        for acct_type, acct_name, net_activity in rows:
            sec = TYPE_TO_SECTION.get(acct_type)
            if not sec:
                continue

            display_amt = _pnl_display_amount(acct_type, net_activity)

            # hierarchy from colon
            acct_path = _parse_path(acct_name)

            # optional subheading by Type within section
            if SHOW_TYPE_SUBHEADINGS and sec in ("Income", "Expenses"):
                acct_path = [acct_type] + acct_path
            elif SHOW_TYPE_SUBHEADINGS and sec == "Cost of Goods Sold":
                acct_path = ["Cost of Goods Sold"] + acct_path

            _add_amount(trees[sec], acct_path, p_idx, display_amt, n)

    # Roll up totals
    for sec in SECTION_ORDER:
        _rollup(trees[sec], n)

    # Flatten sections
    out_sections: List[Dict[str, Any]] = []
    section_totals: Dict[str, List[float]] = {}

    for sec in SECTION_ORDER:
        root = trees[sec]

        # Flatten children of the root, not the root itself
        sec_rows: List[Dict[str, Any]] = []
        for key in sorted(root.children.keys()):
            child = root.children[key]
            sec_rows.extend(_flatten(child, base_path=[], level=0, n_periods=n))

        sec_total = root.total[:n]
        section_totals[sec] = sec_total

        out_sections.append({
            "section": sec,
            "rows": sec_rows,
            "section_totals": sec_total,
            "section_total": sum(sec_total),
        })

    income = section_totals.get("Income", [0.0] * n)
    cogs = section_totals.get("Cost of Goods Sold", [0.0] * n)
    exp = section_totals.get("Expenses", [0.0] * n)

    gross_profit = [income[i] - cogs[i] for i in range(n)]
    net_profit = [gross_profit[i] - exp[i] for i in range(n)]

    return {
        "entity_id": entity_id,
        "periods": [
            {"label": lbl, "start_date": s.isoformat(), "end_date": e.isoformat()}
            for (lbl, s, e) in periods
        ],
        "sections": out_sections,
        "totals": {
            "income": income,
            "cogs": cogs,
            "expenses": exp,
            "gross_profit": gross_profit,
            "net_profit": net_profit,
            "total_net_profit": sum(net_profit),
        },
    }
