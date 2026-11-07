"""
config.py - konfigurasi pusat Real-Time Fraud Detection System
==============================================================
Semua nilai bisa diganti lewat environment variable (dipakai oleh docker-compose),
sehingga kode yang sama berjalan di Docker Desktop maupun langsung di laptop.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
RAW_DIR = Path(os.getenv("RAW_DIR", DATA_DIR / "raw"))            # <- file CSV ada di sini
STREAM_INPUT_DIR = DATA_DIR / "stream_input"                       # dipakai hanya mode --source file (tanpa Kafka)
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", DATA_DIR / "checkpoints"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", ROOT / "models"))
RESULTS_DIR = ROOT / "docs" / "results"

# ---------------------------------------------------------------- sumber data (CSV)
# Producer mencari file dengan pola nama berikut di data/raw/ (huruf besar/kecil diabaikan).
# "label" = nama yang tampil di dasbor; boleh diganti lewat env SOURCE_LABEL_<ID>.
SOURCES = {
    "bank": {
        "patterns": ["bank_transactions*.csv", "bank*.csv"],
        "label": os.getenv("SOURCE_LABEL_BANK", "Transaksi Bank (bank_transactions_data_2)"),
        "has_label": False,     # dataset ini TIDAK punya kolom fraud -> deteksi anomali tanpa label (clustering)
    },
    "banksim": {
        "patterns": ["bs140513*.csv", "banksim*.csv"],
        "label": os.getenv("SOURCE_LABEL_BANKSIM", "Pembayaran Bank (BankSim)"),
        "has_label": True,      # kolom fraud: 1 = fraud
    },
    "card": {
        "patterns": ["creditcard*.csv"],
        "label": os.getenv("SOURCE_LABEL_CARD", "Transaksi Kartu Kredit (creditcard.csv)"),
        "has_label": True,      # kolom Class: 1 = fraud
    },
}


def find_source_file(source_id: str) -> Path | None:
    import fnmatch
    files = sorted(p for p in RAW_DIR.glob("*") if p.is_file() and p.suffix.lower() == ".csv")
    for pat in SOURCES[source_id]["patterns"]:
        hits = [p for p in files if fnmatch.fnmatch(p.name.lower(), pat.lower())]
        if hits:
            return hits[0]
    return None


# ---------------------------------------------------------------- pembagian data (berdasarkan waktu)
# Model dilatih pada data "lama" dan diuji/di-stream pada data "baru" -> evaluasi jujur, tanpa kebocoran.
BANKSIM_TRAIN_MAX_STEP = 107         # BankSim hari 0..107 = latih, 108..143 = validasi, 144..179 = uji + stream
BANKSIM_VALID_MAX_STEP = 143
CARD_TRAIN_FRAC = 0.60               # creditcard diurutkan menurut Time: 60% latih, 20% validasi, 20% uji + stream
CARD_VALID_FRAC = 0.80

# ---------------------------------------------------------------- Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9094")   # di dalam Docker: kafka:9092
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "fraud_alerts")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))

# ---------------------------------------------------------------- Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_KAFKA_PACKAGE = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "2"))

# ---------------------------------------------------------------- PostgreSQL
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "frauddb")
PG_USER = os.getenv("PG_USER", "fraud")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fraud123")

# ---------------------------------------------------------------- keputusan
DECISIONS = {"APPROVE": "AMAN - SETUJUI", "REVIEW": "MENCURIGAKAN - TINJAU", "BLOCK": "FRAUD - BLOKIR"}
