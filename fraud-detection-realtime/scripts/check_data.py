"""check_data.py - memeriksa apakah file CSV sudah berada di data/raw/ dengan kolom yang benar."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

NEED = {"bank": {"TransactionID", "AccountID", "TransactionAmount", "TransactionDate", "TransactionType", "Location",
                 "DeviceID", "IP Address", "MerchantID", "Channel"},
        "card": {"Time", "Amount", "Class", *[f"V{i}" for i in range(1, 29)]},
        "banksim": {"step", "customer", "age", "gender", "merchant", "category", "amount", "fraud"}}
ok, total = True, 0
print(f"folder: {config.RAW_DIR}")
for s, cols in NEED.items():
    p = config.find_source_file(s)
    if not p:
        print(f"  [-] {s:6s}: file tidak ada -> sumber ini dilewati (pola {config.SOURCES[s]['patterns']})")
        continue
    miss = cols - set(pd.read_csv(p, nrows=5).columns)
    print(f"  [{'OK' if not miss else 'X'}] {s:6s}: {p.name} ({p.stat().st_size / 1e6:,.1f} MB)"
          + (f"  kolom hilang: {miss}" if miss else ""))
    ok &= not miss
    with open(p, encoding="utf-8", errors="replace") as fh:
        n = sum(1 for _ in fh) - 1
    total += n
    print(f"         {n:,} baris")
print(f"TOTAL: {total:,} baris -> syarat Manual DIT >= 10.000: {'TERPENUHI' if total >= 10000 else 'BELUM'}")
sys.exit(0 if ok and total >= 10000 else 1)
