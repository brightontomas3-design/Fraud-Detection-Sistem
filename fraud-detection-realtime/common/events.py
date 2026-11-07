"""
events.py - skema event transaksi terpadu (unified event)
==========================================================
Setiap CSV punya kolom yang berbeda. Producer mengubah setiap baris CSV menjadi SATU
event JSON dengan bentuk yang sama, lalu mengirimnya ke Kafka:

{
  "event_id": "CC-0001234",          # id unik (prefix sumber + id asli / nomor baris)
  "source": "bank" | "card" | "banksim",
  "account_id": "...", "counterparty": "...",
  "amount": 123.45, "tx_type": "Debit", "channel": "ATM", "location": "Houston",
  "original_time": "2023-04-11 16:29:14",   # waktu asli di CSV
  "event_time": "2026-10-11T14:05:01.123",  # waktu event dikirim (simulasi real-time)
  "produced_ms": 1760000000000,             # untuk mengukur latency
  "label": 0 | 1 | null,                    # label fraud asli (null jika dataset tidak punya)
  "num": {"V1": -1.35, ...},                # fitur numerik khusus sumber
  "attr": {"device_id": "D000380", ...}     # atribut teks khusus sumber
}
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

import pandas as pd

CARD_V = [f"V{i}" for i in range(1, 29)]


def _f(x):
    """float aman: NaN/inf -> None (JSON tidak boleh NaN)."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _s(x):
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else str(x)


def map_bank(row: pd.Series, idx: int) -> dict:
    return {"event_id": f"BK-{row['TransactionID']}", "source": "bank", "account_id": _s(row.get("AccountID")),
            "counterparty": _s(row.get("MerchantID")), "amount": _f(row.get("TransactionAmount")),
            "tx_type": _s(row.get("TransactionType")), "channel": _s(row.get("Channel")),
            "location": _s(row.get("Location")), "original_time": _s(row.get("TransactionDate")), "label": None,
            "num": {},
            "attr": {"device_id": _s(row.get("DeviceID")), "ip_address": _s(row.get("IP Address")),
                     "merchant_id": _s(row.get("MerchantID"))}}


def map_card(row: pd.Series, idx: int) -> dict:
    # dataset kartu kredit tidak punya id akun/kartu (sudah dianonimkan dengan PCA V1..V28)
    return {"event_id": f"CC-{idx:07d}", "source": "card", "account_id": None, "counterparty": None,
            "amount": _f(row.get("Amount")), "tx_type": "CARD_PAYMENT", "channel": "CARD", "location": None,
            "original_time": f"t+{int(row['Time'])}s", "label": int(row["Class"]),
            "num": {"Time": _f(row["Time"]), **{v: _f(row[v]) for v in CARD_V}}, "attr": {}}



def _q(x):
    """BankSim membungkus teks dengan tanda kutip tunggal: 'C1093826151' -> C1093826151."""
    s = _s(x)
    return s.strip("'") if s else s


def map_banksim(row: pd.Series, idx: int) -> dict:
    return {"event_id": f"BS-{idx:07d}", "source": "banksim", "account_id": _q(row.get("customer")),
            "counterparty": _q(row.get("merchant")), "amount": _f(row.get("amount")),
            "tx_type": _q(row.get("category")), "channel": "PAYMENT", "location": _q(row.get("zipcodeOri")),
            "original_time": f"hari {int(row['step'])}", "label": int(row["fraud"]),
            "num": {"step": _f(row["step"])},
            "attr": {"age": _q(row.get("age")), "gender": _q(row.get("gender")),
                     "merchant_id": _q(row.get("merchant"))}}


MAPPERS = {"bank": map_bank, "card": map_card, "banksim": map_banksim}


def now_iso() -> str:
    """Waktu UTC (tanpa zona) - sama dengan zona waktu Spark dan PostgreSQL di docker-compose."""
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")


def spark_schema():
    """Skema Spark untuk JSON event di atas (dipakai oleh Spark Streaming)."""
    from pyspark.sql.types import (DoubleType, IntegerType, LongType, MapType, StringType, StructField,
                                   StructType)
    return StructType([
        StructField("event_id", StringType()), StructField("source", StringType()),
        StructField("account_id", StringType()), StructField("counterparty", StringType()),
        StructField("amount", DoubleType()), StructField("tx_type", StringType()),
        StructField("channel", StringType()), StructField("location", StringType()),
        StructField("original_time", StringType()), StructField("event_time", StringType()),
        StructField("produced_ms", LongType()), StructField("label", IntegerType()),
        StructField("num", MapType(StringType(), DoubleType())),
        StructField("attr", MapType(StringType(), StringType())),
    ])
