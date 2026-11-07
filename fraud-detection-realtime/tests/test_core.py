"""Unit test inti: pemetaan 2 CSV -> event, pemilihan ambang, dan fitur Spark."""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.events import MAPPERS  # noqa: E402
from spark_jobs.train_models import binary_metrics, choose_thresholds  # noqa: E402


def test_map_bank():
    row = pd.Series({"TransactionID": "TX1", "AccountID": "AC1", "TransactionAmount": 10.5,
                     "TransactionDate": "2023-04-11 16:29:14", "TransactionType": "Debit", "Location": "Houston",
                     "DeviceID": "D1", "IP Address": "1.2.3.4", "MerchantID": "M1", "Channel": "ATM"})
    ev = MAPPERS["bank"](row, 0)
    assert ev["event_id"] == "BK-TX1" and ev["label"] is None and ev["attr"]["device_id"] == "D1"
    json.dumps(ev)


def test_map_card_has_label():
    card = pd.Series({"Time": 10.0, "Amount": 5.0, "Class": 1, **{f"V{i}": 0.1 for i in range(1, 29)}})
    ev = MAPPERS["card"](card, 3)
    assert ev["label"] == 1 and len(ev["num"]) == 29 and ev["event_id"] == "CC-0000003"


def test_map_banksim_strips_quotes():
    row = pd.Series({"step": 150, "customer": "'C1'", "age": "'4'", "gender": "'M'", "zipcodeOri": "'28007'",
                     "merchant": "'M9'", "zipMerchant": "'28007'", "category": "'es_travel'", "amount": 900.0,
                     "fraud": 1})
    ev = MAPPERS["banksim"](row, 5)
    assert ev["account_id"] == "C1" and ev["tx_type"] == "es_travel" and ev["attr"]["age"] == "4"
    assert ev["label"] == 1 and ev["event_id"] == "BS-0000005"


def test_nan_becomes_null():
    row = pd.Series({"TransactionID": "TX2", "AccountID": "AC1", "TransactionAmount": float("nan"),
                     "TransactionDate": None, "TransactionType": "Debit", "Location": None, "DeviceID": None,
                     "IP Address": None, "MerchantID": None, "Channel": None})
    ev = MAPPERS["bank"](row, 0)
    assert ev["amount"] is None
    assert "NaN" not in json.dumps(ev)


def test_thresholds_and_metrics():
    rng = np.random.default_rng(0)
    y = np.r_[np.zeros(1000, int), np.ones(20, int)]
    p = np.r_[rng.uniform(0, .5, 1000), rng.uniform(.6, 1, 20)]
    t = choose_thresholds(p, y)
    assert t["review"] <= t["block"]
    m = binary_metrics(y, (p >= t["block"]).astype(int))
    assert m["recall"] == 1.0 and m["precision"] == 1.0


@pytest.fixture(scope="module")
def spark():
    from spark_jobs.spark_session import get_spark
    s = get_spark("tests", shuffle_partitions=1)
    yield s
    s.stop()


def test_spark_features(spark):
    from spark_jobs import features as FE
    df = spark.createDataFrame([("AC1", 900.0, "Debit", "ATM", "Boston", "D1", "1.1.1.1")],
                               "account_id string, amount double, tx_type string, channel string, location string,"
                               " device_id string, ip_address string")
    prof = {"acct": spark.createDataFrame([("AC1", 100.0, 10.0, 5, ["Houston"])],
                                          "account_id string, acct_mean double, acct_std double, acct_n long,"
                                          " acct_locations array<string>"),
            "device": spark.createDataFrame([("D1", 2)], "device_id string, device_accounts long"),
            "ip": spark.createDataFrame([("1.1.1.1", 1)], "ip_address string, ip_accounts long")}
    r = FE.bank_reasons(FE.bank_features(df, prof, 300.0), 8, 9).first()
    assert r.amt_ratio == 9.0 and r.new_location == 1.0
    assert "JUMLAH_>3x_RATA2_AKUN" in r.reasons and "LOKASI_BARU_UNTUK_AKUN" in r.reasons
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.types import IntegerType, StructField, StructType
    v = spark.createDataFrame([(Vectors.dense([3.0, 4.0]), 0)],
                              StructType([StructField("scaled", VectorUDT()), StructField("cluster", IntegerType())]))
    assert abs(FE.kmeans_distance(v, [[0.0, 0.0]]).first().distance - 5.0) < 1e-9
