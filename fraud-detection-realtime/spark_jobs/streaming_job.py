"""
streaming_job.py - Spark Structured Streaming: Kafka -> bersihkan -> transformasi -> agregasi -> MLlib -> PostgreSQL
===================================================================================================================

    python spark_jobs/streaming_job.py                     # baca dari Kafka (topic "transactions")
    python spark_jobs/streaming_job.py --source file       # tanpa Kafka: baca data/stream_input/ (uji di laptop)

Dua query streaming berjalan bersamaan:

  Query 1 "scoring" (micro-batch tiap 2 detik, foreachBatch):
     parse JSON -> PEMBERSIHAN (tolak id kosong, sumber tak dikenal, jumlah kosong/negatif, waktu rusak)
     -> buang DUPLIKAT -> TRANSFORMASI + feature engineering per sumber
     -> MODEL Spark MLlib per sumber (GBT kartu kredit / GBT BankSim / KMeans bank) -> skor risiko 0..1 -> KEPUTUSAN
        (AMAN-SETUJUI / MENCURIGAKAN-TINJAU / FRAUD-BLOKIR) + alasan
     -> simpan ke PostgreSQL (transactions, predictions, alerts, rejected_records, batch_metrics)
     -> PERINGATAN: transaksi TINJAU/BLOKIR juga dikirim ke topic Kafka "fraud_alerts"

  Query 2 "aggregation": window 1 menit per sumber (jumlah transaksi, total/rata2/maks nominal, fraud berlabel)
     dengan watermark -> tabel agg_minute (upsert)
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common.events import CARD_V, spark_schema  # noqa: E402
from spark_jobs import features as FE  # noqa: E402
from spark_jobs.spark_session import get_spark  # noqa: E402
from storage import db  # noqa: E402

from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

log = logging.getLogger("streaming")
OUT_COLS = ["event_id", "source", "model", "risk_score", "decision", "reasons", "cluster"]


# ---------------------------------------------------------------- memuat model hasil train_models.py
class Models:
    def __init__(self, spark):
        self.m, self.meta = {}, {}
        for src, name in (("card", "card_gbt"), ("bank", "bank_kmeans"), ("banksim", "banksim_gbt")):
            path, meta = config.MODELS_DIR / name, config.MODELS_DIR / f"{src}_meta.json"
            if path.exists() and meta.exists():
                self.m[src] = PipelineModel.load(str(path))
                self.meta[src] = json.loads(meta.read_text(encoding="utf-8"))
                log.info("model %-6s dimuat: %s", src, self.meta[src]["algorithm"])
            else:
                log.warning("model %s belum ada (%s) -> jalankan spark_jobs/train_models.py", src, path)
        if "banksim" in self.m:
            self.banksim_prof = spark.read.parquet(str(config.MODELS_DIR / "banksim_profile_cust")).cache()
        if "bank" in self.m:
            self.bank_prof = {k: spark.read.parquet(str(config.MODELS_DIR / f"bank_profile_{k}")).cache()
                              for k in ("acct", "device", "ip")}


def decide_supervised(df: DataFrame, thr: dict) -> DataFrame:
    return df.withColumn("decision", F.when(F.col("risk_score") >= thr["block"], "BLOCK")
                         .when(F.col("risk_score") >= thr["review"], "REVIEW").otherwise("APPROVE"))


def score_card(df: DataFrame, M: Models) -> DataFrame:
    x = df.select("event_id", "source", "amount", F.col("num")["Time"].alias("Time"),
                  *[F.col("num")[v].alias(v) for v in CARD_V])
    x = FE.card_features(x)
    p = M.m["card"].transform(x).withColumn("risk_score", vector_to_array("probability")[1])
    p = FE.card_reasons(decide_supervised(p, M.meta["card"]["thresholds"]))
    return p.withColumn("model", F.lit("GBT-card")).withColumn("cluster", F.lit(None).cast("int"))


def score_bank(df: DataFrame, M: Models) -> DataFrame:
    meta = M.meta["bank"]
    x = df.select("event_id", "source", "account_id", "amount", "tx_type", "channel", "location",
                  F.col("attr")["device_id"].alias("device_id"), F.col("attr")["ip_address"].alias("ip_address"))
    x = FE.bank_features(x, M.bank_prof, meta["global_mean_amount"])
    p = FE.kmeans_distance(M.m["bank"].transform(x), meta["centers_scaled"])
    t_rev, t_high = meta["thresholds"]["review_distance_p97_5"], meta["thresholds"]["high_distance_p99_5"]
    p = (p.withColumn("risk_score", F.least(F.lit(1.0), F.col("distance") / F.lit(t_high)))
          # tanpa label -> anomali hanya boleh ditinjau manusia, tidak diblokir otomatis
          .withColumn("decision", F.when(F.col("distance") >= t_rev, "REVIEW").otherwise("APPROVE")))
    p = FE.bank_reasons(p, meta["reason_thresholds"]["device_accounts_p95"], meta["reason_thresholds"]["ip_accounts_p95"])
    return p.withColumn("model", F.lit("KMeans-bank")).withColumn("cluster", F.col("cluster").cast("int"))


def score_banksim(df: DataFrame, M: Models) -> DataFrame:
    meta = M.meta["banksim"]
    x = df.select("event_id", "source", "account_id", "amount", "tx_type",
                  F.col("attr")["merchant_id"].alias("merchant_id"), F.col("attr")["age"].alias("age"),
                  F.col("attr")["gender"].alias("gender"))
    x = FE.banksim_features(x, M.banksim_prof, meta["global_mean_amount"])
    p = M.m["banksim"].transform(x).withColumn("risk_score", vector_to_array("probability")[1])
    p = FE.banksim_reasons(decide_supervised(p, meta["thresholds"]), meta["risky_categories"])
    return p.withColumn("model", F.lit("GBT-banksim")).withColumn("cluster", F.lit(None).cast("int"))


SCORERS = {"card": score_card, "bank": score_bank, "banksim": score_banksim}


# ---------------------------------------------------------------- query 1: pembersihan + skor + simpan
def make_batch_handler(M: Models, publish_alerts: bool):
    alert_producer = None
    if publish_alerts:
        from kafka import KafkaProducer
        alert_producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP,
                                       value_serializer=lambda v: json.dumps(v, default=str).encode())

    def handle(batch: DataFrame, batch_id: int):
        t0 = time.time()
        reason = (F.when(F.col("event_id").isNull() & F.col("source").isNull(), "MALFORMED_JSON")
                  .when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
                  .when(~F.col("source").isin(*config.SOURCES), "UNKNOWN_SOURCE")
                  .when(F.col("amount").isNull(), "MISSING_AMOUNT")
                  .when(F.col("amount") < 0, "INVALID_AMOUNT")      # nominal 0 sah (mis. verifikasi kartu)
                  .when(F.to_timestamp("event_time").isNull(), "INVALID_EVENT_TIME"))
        df = batch.withColumn("reject_reason", reason).cache()
        n_in = df.count()
        if n_in == 0:
            df.unpersist()
            return
        rej = df.where(F.col("reject_reason").isNotNull()) \
            .select("event_id", "source", F.col("reject_reason").alias("reason"),
                    F.substring("raw", 1, 1000).alias("payload")).toPandas()
        valid = df.where(F.col("reject_reason").isNull())
        n_valid_raw = valid.count()
        valid = valid.dropDuplicates(["event_id"]).withColumn("event_ts", F.to_timestamp("event_time")).cache()
        n_valid = valid.count()

        parts = []
        for src, fn in SCORERS.items():
            sub = valid.where(F.col("source") == src)
            if src in M.m:
                parts.append(fn(sub, M).select(*OUT_COLS))
            else:
                parts.append(sub.select("event_id", "source", F.lit("NO_MODEL").alias("model"),
                                        F.lit(0.0).alias("risk_score"), F.lit("APPROVE").alias("decision"),
                                        F.lit("model belum dilatih").alias("reasons"),
                                        F.lit(None).cast("int").alias("cluster")))
        scored = parts[0]
        for p in parts[1:]:
            scored = scored.unionByName(p)
        # alasan hanya disimpan untuk transaksi yang ditandai (TINJAU / BLOKIR)
        scored = scored.withColumn("reasons", F.when(F.col("decision") == "APPROVE", F.lit(None))
                                   .otherwise(F.col("reasons")))

        tx = valid.select("event_id", "source", "account_id", "counterparty", F.round("amount", 2).alias("amount"),
                          "tx_type", "channel", "location", F.col("attr")["device_id"].alias("device_id"),
                          F.col("attr")["ip_address"].alias("ip_address"), "original_time",
                          F.col("event_ts").alias("event_time"), "label", "produced_ms").toPandas()
        pr = scored.toPandas()
        now_ms = time.time() * 1000
        lat = (now_ms - tx.set_index("event_id")["produced_ms"]).rename("latency_ms")
        pr = pr.join(lat, on="event_id")
        db.insert_rows("transactions", tx.drop(columns=["produced_ms"]), key="event_id")
        db.insert_rows("predictions", pr, key="event_id")
        al = pr[pr.decision != "APPROVE"].merge(tx[["event_id", "account_id", "amount"]], on="event_id")
        al = al[["event_id", "source", "account_id", "amount", "risk_score", "decision", "reasons"]]
        db.insert_rows("alerts", al, key="event_id")
        if not rej.empty:
            db.insert_rows("rejected_records", rej)
        if alert_producer is not None and not al.empty:
            for r in al.to_dict("records"):
                alert_producer.send(config.KAFKA_ALERT_TOPIC, r)
            alert_producer.flush(10)
        ms = (time.time() - t0) * 1000
        n_rev, n_blk = int((pr.decision == "REVIEW").sum()), int((pr.decision == "BLOCK").sum())
        db.insert_rows("batch_metrics", __import__("pandas").DataFrame([{
            "batch_id": batch_id, "rows_in": n_in, "rows_valid": n_valid, "rows_rejected": len(rej),
            "rows_duplicate": n_valid_raw - n_valid, "n_review": n_rev, "n_block": n_blk,
            "processing_ms": round(ms, 1), "avg_latency_ms": round(float(pr.latency_ms.mean()), 1) if len(pr) else None}]))
        log.info("batch %d: masuk=%d valid=%d ditolak=%d duplikat=%d | TINJAU=%d BLOKIR=%d | %.0f ms",
                 batch_id, n_in, n_valid, len(rej), n_valid_raw - n_valid, n_rev, n_blk, ms)
        df.unpersist(); valid.unpersist()

    return handle


# ---------------------------------------------------------------- query 2: agregasi per menit
def agg_handler(batch: DataFrame, batch_id: int):
    pdf = batch.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                       "source", "tx_count", F.round("total_amount", 2).alias("total_amount"),
                       F.round("avg_amount", 2).alias("avg_amount"), F.round("max_amount", 2).alias("max_amount"),
                       "known_fraud").toPandas()
    if not pdf.empty:
        db.upsert_agg(pdf)


def read_stream(spark, a) -> DataFrame:
    if a.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
               .option("subscribe", config.KAFKA_TOPIC)
               .option("startingOffsets", a.starting_offsets)
               .option("maxOffsetsPerTrigger", a.max_per_trigger)
               .option("failOnDataLoss", "false").load()
               .select(F.col("value").cast("string").alias("raw")))
    else:
        config.STREAM_INPUT_DIR.mkdir(parents=True, exist_ok=True)
        raw = (spark.readStream.format("text").option("maxFilesPerTrigger", 20)
               .load(str(config.STREAM_INPUT_DIR)).select(F.col("value").alias("raw")))
    return raw.select("raw", F.from_json("raw", spark_schema()).alias("e")).select("raw", "e.*")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "file"], default="kafka")
    ap.add_argument("--starting-offsets", default="latest", help="kafka: latest | earliest")
    ap.add_argument("--max-per-trigger", type=int, default=5000)
    ap.add_argument("--trigger", type=int, default=config.TRIGGER_SECONDS)
    ap.add_argument("--reset-checkpoint", action="store_true")
    ap.add_argument("--run-seconds", type=int, default=0, help="berhenti setelah N detik (0 = terus)")
    a = ap.parse_args()

    db.init_schema()
    spark = get_spark("fraud-streaming", kafka=a.source == "kafka")
    M = Models(spark)
    ck = config.CHECKPOINT_DIR / a.source
    if a.reset_checkpoint:
        shutil.rmtree(ck, ignore_errors=True)

    events = read_stream(spark, a)
    q1 = (events.writeStream.foreachBatch(make_batch_handler(M, publish_alerts=a.source == "kafka"))
          .option("checkpointLocation", str(ck / "scoring")).trigger(processingTime=f"{a.trigger} seconds")
          .queryName("scoring").start())

    valid = (events.where(F.col("event_id").isNotNull() & F.col("source").isin(*config.SOURCES)
                          & (F.col("amount") >= 0))
             .withColumn("event_ts", F.to_timestamp("event_time")).where(F.col("event_ts").isNotNull()))
    agg = (valid.withWatermark("event_ts", "1 minute").dropDuplicatesWithinWatermark(["event_id"])
           .groupBy(F.window("event_ts", "1 minute"), "source")
           .agg(F.count("*").alias("tx_count"), F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount"), F.max("amount").alias("max_amount"),
                F.sum(F.coalesce(F.col("label"), F.lit(0))).alias("known_fraud")))
    q2 = (agg.writeStream.outputMode("update").foreachBatch(agg_handler)
          .option("checkpointLocation", str(ck / "aggregation")).trigger(processingTime="10 seconds")
          .queryName("aggregation").start())
    log.info("streaming query started (source=%s, trigger=%ss) -> PostgreSQL %s:%s/%s", a.source, a.trigger,
             config.PG_HOST, config.PG_PORT, config.PG_DB)
    try:
        if a.run_seconds:
            time.sleep(a.run_seconds)
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for q in (q1, q2):
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
