"""
send_fraud_demo.py - kirim SATU transaksi fraud asli (berlabel) dari dataset untuk demo langsung
==============================================================================================

    python producer/send_fraud_demo.py --source banksim    # fraud asli BankSim (bagian uji)
    python producer/send_fraud_demo.py --source card       # fraud asli kartu kredit (bagian uji)
    python producer/send_fraud_demo.py --source bank       # transaksi bank paling anomali (tanpa label)
    python producer/send_fraud_demo.py --source card --sink file

Baris diambil apa adanya dari CSV (tidak dibuat-buat); hanya event_id diberi akhiran -DEMO-xxxx
agar tidak dianggap duplikat. Dalam beberapa detik peringatan muncul di dasbor.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common.events import MAPPERS, now_iso  # noqa: E402
from producer.csv_producer import FileSink, KafkaSink  # noqa: E402


def pick(source: str, rnd: random.Random) -> dict:
    path = config.find_source_file(source)
    if not path:
        raise SystemExit(f"file sumber '{source}' tidak ditemukan di {config.RAW_DIR}")
    if source == "card":
        df = pd.read_csv(path)
        t80 = float(np.quantile(df["Time"], config.CARD_VALID_FRAC, method="inverted_cdf"))
        cand = df[(df["Class"] == 1) & (df["Time"] >= t80)]
    elif source == "banksim":
        df = pd.read_csv(path, quotechar="'")
        cand = df[(df["fraud"] == 1) & (df["step"] > config.BANKSIM_VALID_MAX_STEP)]
    else:
        meta = json.loads((config.MODELS_DIR / "bank_meta.json").read_text(encoding="utf-8"))
        ids = [r["tx_id"] for r in meta["top_anomalies"]]
        df = pd.read_csv(path)
        cand = df[df["TransactionID"].isin(ids)]
    i = rnd.choice(list(cand.index))
    ev = MAPPERS[source](cand.loc[i], int(i))
    ev["event_id"] = f"{ev['event_id']}-DEMO-{uuid.uuid4().hex[:4]}"
    return ev


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["card", "banksim", "bank"], default="banksim")
    ap.add_argument("--sink", choices=["kafka", "file"], default="kafka")
    ap.add_argument("--n", type=int, default=1)
    a = ap.parse_args()
    rnd = random.Random()
    sink = KafkaSink() if a.sink == "kafka" else FileSink(every=1)
    for _ in range(a.n):
        ev = dict(pick(a.source, rnd), event_time=now_iso(), produced_ms=int(time.time() * 1000))
        sink.send(ev)
        print(f"TERKIRIM {ev['event_id']} | sumber={ev['source']} | nominal={ev['amount']} | label={ev['label']}")
    sink.close()


if __name__ == "__main__":
    main()
