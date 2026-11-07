"""
csv_producer.py - membaca file CSV dan mengirimnya ke Kafka seolah-olah transaksi real-time
=============================================================================================

    python producer/csv_producer.py                         # ke Kafka, 20 transaksi/detik, campuran beberapa sumber
    python producer/csv_producer.py --rate 50 --limit 5000
    python producer/csv_producer.py --sources card          # satu sumber saja
    python producer/csv_producer.py --sink file             # tanpa Kafka (untuk uji di laptop)

Alur: CSV (data/raw) -> pandas (dibaca per potongan/chunk, hemat memori) -> event JSON terpadu
      -> KafkaProducer -> topic "transactions" (3 partisi, key = account_id) -> Spark Streaming

Secara default yang dikirim hanya bagian UJI dari data berlabel (bukan data yang dipakai melatih model),
supaya hasil deteksi di dasbor jujur. Pakai --split all untuk mengirim semua baris.
"""
from __future__ import annotations

import argparse
import json
import logging
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

log = logging.getLogger("producer")


# ---------------------------------------------------------------- pembaca CSV per sumber (generator)
def read_bank(path: Path, split: str):
    df = pd.read_csv(path)
    for i, row in df.iterrows():
        yield MAPPERS["bank"](row, i)


def read_card(path: Path, split: str):
    df = pd.read_csv(path)
    if split == "test":
        t80 = float(np.quantile(df["Time"], config.CARD_VALID_FRAC, method="inverted_cdf"))
        df = df[df["Time"] >= t80]
    for i, row in df.iterrows():
        yield MAPPERS["card"](row, i)


def read_banksim(path: Path, split: str):
    df = pd.read_csv(path, quotechar="'")                         # teks BankSim dibungkus '...'
    if split == "test":
        df = df[df["step"] > config.BANKSIM_VALID_MAX_STEP]
    for i, row in df.iterrows():
        yield MAPPERS["banksim"](row, i)


READERS = {"bank": read_bank, "card": read_card, "banksim": read_banksim}


# ---------------------------------------------------------------- data kotor (untuk menguji pembersihan Spark)
def make_dirty(ev: dict, rnd: random.Random) -> dict:
    """Salinan rusak yang DITANDAI (attr.synthetic_dirty=1) - Spark harus menolaknya."""
    bad = dict(ev, attr=dict(ev.get("attr") or {}, synthetic_dirty="1"))
    kind = rnd.choice(["neg_amount", "null_amount", "null_id", "bad_source"])
    if kind == "neg_amount":
        bad["amount"] = -(abs(ev["amount"] or 0.0) + 1.0)
    elif kind == "null_amount":
        bad["amount"] = None
    elif kind == "null_id":
        bad["event_id"] = None
    else:
        bad["source"] = "unknown"
    bad["event_id"] = None if kind == "null_id" else f"{ev['event_id']}-DIRTY-{uuid.uuid4().hex[:6]}"
    return bad


# ---------------------------------------------------------------- tujuan pengiriman
class KafkaSink:
    def __init__(self):
        from kafka import KafkaProducer
        self.p = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP, acks=1, linger_ms=20,
                               key_serializer=lambda k: (k or "").encode(),
                               value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())
        self.sent = 0

    def send(self, ev: dict):
        self.p.send(config.KAFKA_TOPIC, key=ev.get("account_id") or ev.get("source"), value=ev)
        self.sent += 1

    def close(self):
        self.p.flush(30)
        self.p.close()


class FileSink:
    """Mode tanpa Kafka: tulis file JSON-lines kecil ke data/stream_input/ (dibaca Spark --source file)."""

    def __init__(self, every: int = 50):
        config.STREAM_INPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.buf, self.every, self.sent, self.n = [], every, 0, 0

    def send(self, ev: dict):
        self.buf.append(json.dumps(ev, ensure_ascii=False))
        self.sent += 1
        if len(self.buf) >= self.every:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        self.n += 1
        tmp = config.STREAM_INPUT_DIR / f".part-{time.time_ns()}.tmp"
        tmp.write_text("\n".join(self.buf) + "\n", encoding="utf-8")
        tmp.rename(config.STREAM_INPUT_DIR / f"events-{time.time_ns()}.json")   # rename = atomik
        self.buf = []

    def close(self):
        self.flush()


# ---------------------------------------------------------------- campur sumber sesuai bobot
def mixed_stream(sources: list[str], weights: dict, split: str, loop: bool, seed: int):
    rnd = random.Random(seed)
    gens = {}
    for s in sources:
        path = config.find_source_file(s)
        if not path:
            log.warning("sumber '%s' dilewati: file tidak ditemukan di %s (pola %s)", s, config.RAW_DIR,
                        config.SOURCES[s]["patterns"])
            continue
        log.info("sumber %-6s <- %s", s, path.name)
        gens[s] = READERS[s](path, split)
    while gens:
        names = list(gens)
        s = rnd.choices(names, weights=[weights.get(n, 1.0) for n in names])[0]
        try:
            yield next(gens[s])
        except StopIteration:
            if loop:
                gens[s] = READERS[s](config.find_source_file(s), split)
            else:
                log.info("sumber %s selesai dikirim", s)
                del gens[s]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--sink", choices=["kafka", "file"], default="kafka")
    ap.add_argument("--rate", type=float, default=20.0, help="transaksi per detik (0 = secepatnya)")
    ap.add_argument("--limit", type=int, default=0, help="maksimum transaksi (0 = semua)")
    ap.add_argument("--sources", default="bank,card,banksim")
    ap.add_argument("--mix", default="bank=1,card=3,banksim=3", help="bobot campuran sumber")
    ap.add_argument("--split", choices=["test", "all"], default="test")
    ap.add_argument("--dirty-rate", type=float, default=0.01, help="proporsi salinan data rusak (ditandai)")
    ap.add_argument("--duplicate-rate", type=float, default=0.005, help="proporsi event yang dikirim ulang")
    ap.add_argument("--loop", action="store_true", help="ulang dari awal jika sumber habis")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    weights = {k: float(v) for k, v in (kv.split("=") for kv in a.mix.split(","))}
    sources = [s.strip() for s in a.sources.split(",") if s.strip()]
    sink = KafkaSink() if a.sink == "kafka" else FileSink()
    rnd = random.Random(a.seed)
    target = f"Kafka {config.KAFKA_BOOTSTRAP} topic '{config.KAFKA_TOPIC}'" if a.sink == "kafka" \
        else str(config.STREAM_INPUT_DIR)
    log.info("mulai mengirim -> %s | %.0f tx/detik | split=%s", target, a.rate, a.split)
    t0, n, per = time.perf_counter(), 0, {}
    try:
        for ev in mixed_stream(sources, weights, a.split, a.loop, a.seed):
            batch = [ev]
            if rnd.random() < a.duplicate_rate:
                batch.append(ev)                        # duplikat persis -> harus dibuang Spark
            if rnd.random() < a.dirty_rate:
                batch.append(make_dirty(ev, rnd))       # data rusak -> harus ditolak Spark
            for e in batch:
                e = dict(e, event_time=now_iso(), produced_ms=int(time.time() * 1000))
                sink.send(e)
            n += 1
            per[ev["source"]] = per.get(ev["source"], 0) + 1
            if a.rate > 0:
                wait = t0 + n / a.rate - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
            if n % 500 == 0:
                log.info("terkirim %d transaksi %s (%.1f tx/detik)", n, per, n / (time.perf_counter() - t0))
            if a.limit and n >= a.limit:
                break
    except KeyboardInterrupt:
        log.info("dihentikan pengguna")
    finally:
        sink.close()
    log.info("selesai: %d transaksi (+duplikat/rusak, total pesan %d) %s dalam %.1f detik",
             n, sink.sent, per, time.perf_counter() - t0)


if __name__ == "__main__":
    main()
