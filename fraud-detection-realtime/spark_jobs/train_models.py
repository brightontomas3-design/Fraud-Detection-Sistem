"""
train_models.py - melatih & mengevaluasi model Spark MLlib untuk setiap sumber data
==============================================================================

    python spark_jobs/train_models.py              # semua sumber
    python spark_jobs/train_models.py --only card  # satu sumber saja

Sumber          | Label fraud? | Teknik MLlib                         | Keluaran
----------------|--------------|--------------------------------------|---------------------------------
card            | ya (Class)   | Klasifikasi: GBTClassifier (+ LR)    | probabilitas fraud 0..1
banksim         | ya (fraud)   | Klasifikasi: GBTClassifier           | probabilitas fraud 0..1
bank            | TIDAK        | Clustering: KMeans + jarak ke pusat  | skor anomali 0..1

Data dibagi menurut WAKTU (bukan acak): latih -> validasi (memilih ambang) -> uji (melaporkan hasil).
Semua angka di docs/results/model_evaluation.json adalah hasil eksekusi nyata, bukan karangan.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common.events import CARD_V  # noqa: E402
from spark_jobs import features as FE  # noqa: E402
from spark_jobs.spark_session import get_spark  # noqa: E402

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import GBTClassifier, LogisticRegression  # noqa: E402
from pyspark.ml.clustering import KMeans  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator  # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

SEED = 42


# ---------------------------------------------------------------- utilitas metrik
def binary_metrics(y: np.ndarray, flag: np.ndarray) -> dict:
    tp = int(((flag == 1) & (y == 1)).sum()); fp = int(((flag == 1) & (y == 0)).sum())
    fn = int(((flag == 0) & (y == 1)).sum()); tn = int(((flag == 0) & (y == 0)).sum())
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return {"precision": round(p, 4), "recall": round(r, 4), "f1": round(2 * p * r / (p + r) if p + r else 0.0, 4),
            "false_positive_rate": round(fp / (fp + tn) if fp + tn else 0.0, 5), "tp": tp, "fp": fp, "fn": fn, "tn": tn}


def choose_thresholds(prob: np.ndarray, y: np.ndarray, review_recall: float = 0.90) -> dict:
    """Ambang BLOKIR = F1 maksimum (validasi). Ambang TINJAU = menangkap >= 90% fraud validasi, tetapi
    tidak lebih rendah dari persentil 99,5 transaksi normal (agar antrean tinjauan tetap kecil)."""
    cand = np.unique(np.quantile(prob, np.linspace(0.5, 0.99999, 2000)))
    cand = np.unique(np.concatenate([cand, prob[y == 1]]))
    best_f1, t_block = -1.0, 0.5
    for t in cand:
        f1 = binary_metrics(y, (prob >= t).astype(int))["f1"]
        if f1 > best_f1:
            best_f1, t_block = f1, float(t)
    fraud_probs = np.sort(prob[y == 1])
    t_review = float(fraud_probs[int(np.floor((1 - review_recall) * len(fraud_probs)))]) if len(fraud_probs) else 0.5
    # batasi beban tinjauan manual: maksimum ~0.5% transaksi normal boleh masuk antrean TINJAU
    t_review = max(t_review, float(np.quantile(prob[y == 0], 0.995)))
    t_review = min(t_review, t_block * 0.999)
    return {"block": round(t_block, 6), "review": round(max(t_review, 1e-6), 6), "valid_best_f1": best_f1}


def eval_classifier(pred, label_col: str, thr: dict) -> dict:
    auc_roc = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction",
                                            metricName="areaUnderROC").evaluate(pred)
    auc_pr = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction",
                                           metricName="areaUnderPR").evaluate(pred)
    pdf = pred.select(vector_to_array("probability")[1].alias("p"), F.col(label_col).alias("y")).toPandas()
    p, y = pdf.p.to_numpy(), pdf.y.to_numpy().astype(int)
    return {"rows": int(len(y)), "fraud": int(y.sum()), "fraud_rate_pct": round(100 * y.mean(), 4),
            "roc_auc": round(auc_roc, 4), "pr_auc": round(auc_pr, 4),
            "BLOCK_only (prob >= ambang blokir)": binary_metrics(y, (p >= thr["block"]).astype(int)),
            "REVIEW_or_BLOCK (prob >= ambang tinjau)": binary_metrics(y, (p >= thr["review"]).astype(int))}


def save_model(model, name: str):
    out = config.MODELS_DIR / name
    shutil.rmtree(out, ignore_errors=True)
    model.write().overwrite().save(str(out))
    return out


# ---------------------------------------------------------------- 1. kartu kredit
def train_card(spark) -> dict:
    path = config.find_source_file("card")
    if not path:
        return {"skipped": "file creditcard*.csv tidak ditemukan di data/raw"}
    t0 = time.time()
    df = (spark.read.csv(str(path), header=True, inferSchema=True)
          .withColumnRenamed("Amount", "amount").withColumn("label", F.col("Class").cast("int")))
    df = FE.card_features(df).cache()
    q60, q80 = df.approxQuantile("Time", [config.CARD_TRAIN_FRAC, config.CARD_VALID_FRAC], 0.0)
    train, valid, test = df.where(F.col("Time") < q60), df.where((F.col("Time") >= q60) & (F.col("Time") < q80)), \
        df.where(F.col("Time") >= q80)
    asm = VectorAssembler(inputCols=FE.CARD_NUM_FEATURES, outputCol="features", handleInvalid="keep")
    gbt = Pipeline(stages=[asm, GBTClassifier(labelCol="label", featuresCol="features", maxIter=60, maxDepth=4,
                                              seed=SEED)]).fit(train)
    # pembanding: regresi logistik dengan bobot kelas
    w = train.where("label=0").count() / max(train.where("label=1").count(), 1)
    lr = Pipeline(stages=[asm, StandardScaler(inputCol="features", outputCol="sf"),
                          LogisticRegression(labelCol="label", featuresCol="sf", weightCol="w", maxIter=100)]) \
        .fit(train.withColumn("w", F.when(F.col("label") == 1, F.lit(w)).otherwise(F.lit(1.0))))
    out = {}
    for name, m in (("GBTClassifier", gbt), ("LogisticRegression (pembanding)", lr)):
        vp = m.transform(valid).select(vector_to_array("probability")[1].alias("p"), "label").toPandas()
        thr = choose_thresholds(vp.p.to_numpy(), vp.label.to_numpy().astype(int))
        out[name] = {"thresholds": thr, "test": eval_classifier(m.transform(test), "label", thr)}
    save_model(gbt, "card_gbt")
    meta = {"source": "card", "algorithm": "GBTClassifier (Spark MLlib)",
            "thresholds": out["GBTClassifier"]["thresholds"], "test": out["GBTClassifier"]["test"],
            "comparison": out, "split": {"by": "Time", "train": f"Time < {q60:.0f}",
                                         "valid": f"{q60:.0f} <= Time < {q80:.0f}",
                                         "test": f"Time >= {q80:.0f} (juga data yang di-stream)",
                                         "train_rows": train.count(), "train_fraud": train.where('label=1').count()},
            "train_seconds": round(time.time() - t0, 1)}
    df.unpersist()
    return meta


# ---------------------------------------------------------------- 2. bank (tanpa label) -> clustering
def train_bank(spark) -> dict:
    path = config.find_source_file("bank")
    if not path:
        return {"skipped": "file bank_transactions*.csv tidak ditemukan di data/raw"}
    t0 = time.time()
    raw = spark.read.csv(str(path), header=True, inferSchema=True)
    df = raw.select(F.col("TransactionID").alias("tx_id"), F.col("AccountID").alias("account_id"),
                    F.col("TransactionAmount").cast("double").alias("amount"),
                    F.col("TransactionType").alias("tx_type"), F.col("Channel").alias("channel"),
                    F.col("Location").alias("location"), F.col("DeviceID").alias("device_id"),
                    F.col("`IP Address`").alias("ip_address")).where("amount > 0").cache()
    prof = FE.bank_profiles(df)
    gmean = df.agg(F.avg("amount")).first()[0]
    feats = FE.bank_features(df, prof, gmean).cache()
    asm = VectorAssembler(inputCols=FE.BANK_NUM_FEATURES, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="scaled", withMean=True, withStd=True)
    pre = Pipeline(stages=[asm, scaler]).fit(feats)
    scaled = pre.transform(feats).cache()
    ev = ClusteringEvaluator(featuresCol="scaled", predictionCol="cluster")
    sil = {}
    for k in range(3, 9):
        km = KMeans(featuresCol="scaled", predictionCol="cluster", k=k, seed=SEED).fit(scaled)
        sil[k] = round(ev.evaluate(km.transform(scaled)), 4)
    best_k = max(sil, key=sil.get)
    # model akhir = satu Pipeline (assembler -> scaler -> KMeans) yang juga dipakai saat streaming
    pm = Pipeline(stages=[asm, scaler, KMeans(featuresCol="scaled", predictionCol="cluster", k=best_k, seed=SEED)]) \
        .fit(feats)
    centers = [c.tolist() for c in pm.stages[-1].clusterCenters()]
    scored = FE.kmeans_distance(pm.transform(feats), centers)
    d = np.array(scored.select("distance").toPandas().distance)
    p975, p995 = float(np.quantile(d, 0.975)), float(np.quantile(d, 0.995))
    sizes = {int(r["cluster"]): int(r["count"]) for r in scored.groupBy("cluster").count().collect()}
    (dev_p95,), (ip_p95,) = feats.approxQuantile(["device_accounts", "ip_accounts"], [0.95], 0.0)
    # simpan model + profil perilaku (dipakai streaming untuk join)
    save_model(pm, "bank_kmeans")
    for name, pdf in prof.items():
        out = config.MODELS_DIR / f"bank_profile_{name}"
        shutil.rmtree(out, ignore_errors=True)
        pdf.write.mode("overwrite").parquet(str(out))
    flagged = scored.where(F.col("distance") >= p975)
    top = (FE.bank_reasons(flagged, dev_p95, ip_p95).orderBy(F.desc("distance"))
           .select("tx_id", "account_id", "amount", "channel", "location", F.round("distance", 3).alias("distance"),
                   "reasons").limit(10).toPandas().to_dict("records"))
    meta = {"source": "bank", "algorithm": "KMeans clustering (Spark MLlib) + jarak ke pusat cluster",
            "label_available": False, "rows": int(df.count()), "features": FE.BANK_NUM_FEATURES,
            "k_selected": best_k, "silhouette_by_k": sil, "cluster_sizes": sizes, "centers_scaled": centers,
            "global_mean_amount": round(gmean, 4),
            "thresholds": {"review_distance_p97_5": round(p975, 4), "high_distance_p99_5": round(p995, 4)},
            "reason_thresholds": {"device_accounts_p95": dev_p95, "ip_accounts_p95": ip_p95},
            "flagged_review_rows": int(flagged.count()), "flagged_pct": round(100 * flagged.count() / len(d), 2),
            "top_anomalies": top,
            "note": "Dataset ini tidak memiliki label fraud. Hasilnya adalah ANOMALI (transaksi paling tidak biasa), "
                    "bukan fraud yang terbukti; karena itu keputusan maksimal = TINJAU, tidak pernah BLOKIR otomatis.",
            "train_seconds": round(time.time() - t0, 1)}
    return meta


# ---------------------------------------------------------------- 3. BankSim (pembayaran bank, berlabel)
def train_banksim(spark) -> dict:
    path = config.find_source_file("banksim")
    if not path:
        return {"skipped": "file BankSim (bs140513*.csv) tidak ditemukan di data/raw"}
    t0 = time.time()
    raw = spark.read.option("quote", "'").csv(str(path), header=True, inferSchema=True)   # teks dibungkus '...'
    df = raw.select(F.col("step").cast("int").alias("step"), F.col("customer").alias("account_id"),
                    F.col("age").cast("string").alias("age"), F.col("gender").alias("gender"),
                    F.col("merchant").alias("merchant_id"), F.col("category").alias("tx_type"),
                    F.col("amount").cast("double").alias("amount"), F.col("fraud").cast("int").alias("label")).cache()
    train = df.where(F.col("step") <= config.BANKSIM_TRAIN_MAX_STEP)
    valid = df.where((F.col("step") > config.BANKSIM_TRAIN_MAX_STEP) & (F.col("step") <= config.BANKSIM_VALID_MAX_STEP))
    test = df.where(F.col("step") > config.BANKSIM_VALID_MAX_STEP)
    prof = FE.banksim_profiles(train)
    gmean = train.agg(F.avg("amount")).first()[0]
    ftrain, fvalid, ftest = (FE.banksim_features(x, prof, gmean) for x in (train, valid, test))
    pipe = Pipeline(stages=[
        StringIndexer(inputCols=["tx_type", "merchant_id"], outputCols=["cat_idx", "mer_idx"], handleInvalid="keep"),
        OneHotEncoder(inputCols=["cat_idx", "mer_idx"], outputCols=["cat_oh", "mer_oh"], handleInvalid="keep"),
        VectorAssembler(inputCols=FE.BANKSIM_NUM_FEATURES + ["cat_oh", "mer_oh"], outputCol="features",
                        handleInvalid="keep"),
        GBTClassifier(labelCol="label", featuresCol="features", maxIter=50, maxDepth=5, seed=SEED)])
    model = pipe.fit(ftrain)
    vp = model.transform(fvalid).select(vector_to_array("probability")[1].alias("p"), "label").toPandas()
    thr = choose_thresholds(vp.p.to_numpy(), vp.label.to_numpy().astype(int))
    res = eval_classifier(model.transform(ftest), "label", thr)
    # pembanding sederhana: aturan "kategori berisiko ATAU nominal >= 500"
    cat_rate = train.groupBy("tx_type").agg(F.avg("label").alias("r")).toPandas()
    risky = sorted(cat_rate[cat_rate.r >= 0.10].tx_type.tolist())
    rule = test.select("label", (F.col("tx_type").isin(*risky) | (F.col("amount") >= 500)).cast("int").alias("f")) \
        .toPandas()
    baseline = binary_metrics(rule.label.to_numpy(), rule.f.to_numpy())
    save_model(model, "banksim_gbt")
    out = config.MODELS_DIR / "banksim_profile_cust"
    shutil.rmtree(out, ignore_errors=True)
    prof.write.mode("overwrite").parquet(str(out))
    meta = {"source": "banksim", "algorithm": "GBTClassifier (Spark MLlib)", "thresholds": thr, "test": res,
            "baseline_rule_category_or_amount": {"rule": f"kategori in {risky} ATAU nominal >= 500", **baseline},
            "risky_categories": risky, "global_mean_amount": round(gmean, 4),
            "split": {"train_days": f"0..{config.BANKSIM_TRAIN_MAX_STEP}",
                      "valid_days": f"{config.BANKSIM_TRAIN_MAX_STEP + 1}..{config.BANKSIM_VALID_MAX_STEP}",
                      "test_days": f"{config.BANKSIM_VALID_MAX_STEP + 1}..179 (juga data yang di-stream)",
                      "train_rows": train.count(), "train_fraud": train.where("label=1").count()},
            "train_seconds": round(time.time() - t0, 1)}
    df.unpersist()
    return meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["card", "bank", "banksim"])
    a = ap.parse_args()
    config.MODELS_DIR.mkdir(parents=True, exist_ok=True)
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("train-models", shuffle_partitions=8)
    out_file = config.RESULTS_DIR / "model_evaluation.json"
    results = json.loads(out_file.read_text(encoding="utf-8")) if out_file.exists() else {}
    results["type"] = "FACTUAL RESULT - dihitung dari eksekusi nyata pada data uji (split waktu)"
    for name, fn in (("bank", train_bank), ("card", train_card), ("banksim", train_banksim)):
        if a.only and a.only != name:
            continue
        print(f"\n=== melatih model sumber '{name}' ...", flush=True)
        meta = fn(spark)
        meta["trained_at"] = datetime.now().isoformat(timespec="seconds")
        results[name] = meta
        if "skipped" not in meta:
            (config.MODELS_DIR / f"{name}_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(json.dumps({k: v for k, v in meta.items() if k in ("algorithm", "thresholds", "test", "skipped",
                                                                  "k_selected", "flagged_pct")}, indent=2), flush=True)
        out_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
    spark.stop()
    print(f"\nselesai -> {out_file}")


if __name__ == "__main__":
    main()
