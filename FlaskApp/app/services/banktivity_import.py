from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class CsvLine:
    csv_account_name: str
    is_debit: bool
    amount: float
    memo: Optional[str] = None


@dataclass
class CsvTransaction:
    trn_no: int
    date: date
    type: Optional[str]
    payee: Optional[str]
    details: Optional[str]
    total_abs: float
    lines: List[CsvLine]


def _parse_date(d: str) -> date:
    # Banktivity export format: dd/mm/YYYY
    return datetime.strptime(str(d), "%d/%m/%Y").date()


def parse_banktivity_csv(csv_path: str, start_date: Optional[date] = None) -> List[CsvTransaction]:
    """Parse Banktivity Transactions CSV into grouped transactions (one per TRN_NO).

    This importer is intentionally conservative:
    - We only import the *bank-side* (asset account) line.
    - Split/category lines (TYPE == '-split-') are ignored; you'll categorise later in-app.
    """
    df = pd.read_csv(csv_path)

    required = ["TRN_NO", "DATE", "ACCOUNT", "AMOUNT", "TYPE", "PAYEE", "DETAILS"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in CSV")

    df = df[df["TRN_NO"].notna()].copy()
    df["TRN_NO"] = df["TRN_NO"].astype(int)
    df["DATE"] = df["DATE"].apply(_parse_date)
    df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0.0)

    if start_date:
        df = df[df["DATE"] >= start_date].copy()

    # Only keep the primary lines (bank account side). Banktivity exports splits as TYPE == '-split-'
    df_main = df[df["TYPE"].astype(str).str.lower() != "-split-"].copy()

    # Some exports repeat the primary line once per split; dedupe per TRN_NO keeping the first.
    df_main.sort_values(["DATE", "TRN_NO"], inplace=True)
    df_main = df_main.drop_duplicates(subset=["TRN_NO"], keep="first")

    transactions: List[CsvTransaction] = []
    for _, row in df_main.iterrows():
        trn_no = int(row["TRN_NO"])
        txn_date = row["DATE"]
        acct = str(row["ACCOUNT"]).strip()
        amt = float(row["AMOUNT"])
        total_abs = abs(amt)

        line = CsvLine(
            csv_account_name=acct,
            is_debit=amt > 0,  # deposits increase asset (debit); withdrawals decrease asset (credit)
            amount=total_abs,
            memo=str(row["DETAILS"]) if pd.notna(row["DETAILS"]) else None,
        )

        transactions.append(
            CsvTransaction(
                trn_no=trn_no,
                date=txn_date,
                type=str(row["TYPE"]) if pd.notna(row["TYPE"]) else None,
                payee=str(row["PAYEE"]) if pd.notna(row["PAYEE"]) else None,
                details=str(row["DETAILS"]) if pd.notna(row["DETAILS"]) else None,
                total_abs=total_abs,
                lines=[line],
            )
        )

    transactions.sort(key=lambda t: (t.date, t.trn_no))
    return transactions
