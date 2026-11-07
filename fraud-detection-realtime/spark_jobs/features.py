"""
features.py - feature engineering bersama (dipakai saat TRAINING dan saat STREAMING)
=====================================================================================
Fungsi yang sama dipakai di kedua tempat supaya fitur yang dilihat model saat
dilatih identik dengan fitur saat transaksi masuk secara real-time.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.events import CARD_V

# ---------------------------------------------------------------- sumber "banksim" (BankSim, berlabel)
BANKSIM_NUM_FEATURES = ["amount", "log_amount", "amt_ratio", "amt_z", "age_num", "cust_n", "is_male"]


def banksim_profiles(train: DataFrame) -> DataFrame:
    """Kebiasaan belanja tiap pelanggan pada periode latih (dipakai saat streaming lewat join)."""
    return train.groupBy("account_id").agg(F.avg("amount").alias("cust_mean"), F.stddev_pop("amount").alias("cust_std"),
                                           F.count("*").alias("cust_n"))


def banksim_features(df: DataFrame, prof: DataFrame, global_mean: float) -> DataFrame:
    """Butuh kolom: account_id, amount, tx_type (kategori belanja), merchant_id, age, gender."""
    out = df.join(F.broadcast(prof), "account_id", "left")
    mean = F.coalesce(F.col("cust_mean"), F.lit(global_mean))
    std = F.when(F.coalesce(F.col("cust_std"), F.lit(0.0)) > 1, F.col("cust_std")).otherwise(F.lit(global_mean))
    return (out.withColumn("log_amount", F.log1p("amount"))
               .withColumn("amt_ratio", F.col("amount") / mean)
               .withColumn("amt_z", (F.col("amount") - mean) / std)
               .withColumn("age_num", F.when(F.col("age").rlike("^[0-9]$"), F.col("age").cast("double"))
                           .otherwise(F.lit(-1.0)))
               .withColumn("is_male", (F.col("gender") == "M").cast("double"))
               .withColumn("cust_n", F.coalesce(F.col("cust_n").cast("double"), F.lit(0.0))))


def banksim_reasons(df: DataFrame, risky_categories: list[str]) -> DataFrame:
    return df.withColumn("reasons", F.concat_ws(", ",
        F.when(F.col("tx_type").isin(*risky_categories), F.concat(F.lit("KATEGORI_BERISIKO_"), F.col("tx_type"))),
        F.when(F.col("amt_ratio") >= 3, F.lit("JUMLAH_>3x_RATA2_PELANGGAN")),
        F.when(F.col("amount") >= 500, F.lit("JUMLAH_BESAR")),
        F.when(F.col("cust_n") == 0, F.lit("PELANGGAN_TANPA_RIWAYAT"))))


# ---------------------------------------------------------------- sumber "card" (creditcard.csv)
CARD_NUM_FEATURES = CARD_V + ["log_amount", "hour"]


def card_features(df: DataFrame) -> DataFrame:
    """Butuh kolom: amount, Time, V1..V28."""
    return (df.withColumn("log_amount", F.log1p(F.col("amount")))
              .withColumn("hour", F.floor(F.col("Time") / 3600) % 24))


def card_reasons(df: DataFrame) -> DataFrame:
    # V1..V28 adalah hasil PCA (anonim) -> tidak bisa diterjemahkan ke alasan bisnis.
    extreme = " OR ".join(f"abs({v}) > 10" for v in CARD_V)
    return df.withColumn("reasons", F.concat_ws(", ",
        F.lit("POLA_TRANSAKSI_KARTU_MIRIP_FRAUD(model)"),
        F.when(F.expr(extreme), F.lit("NILAI_FITUR_PCA_EKSTREM")),
        F.when(F.col("amount") >= 1000, F.lit("JUMLAH_BESAR"))))


# ---------------------------------------------------------------- sumber "bank" (tanpa label)
BANK_NUM_FEATURES = ["log_amount", "amt_ratio", "amt_z", "is_debit", "ch_atm", "ch_online", "ch_branch",
                     "device_accounts", "ip_accounts", "new_location", "acct_n"]


def bank_profiles(train: DataFrame) -> dict[str, DataFrame]:
    """Profil perilaku 'normal' yang dipelajari dari data historis (dipakai saat streaming lewat join)."""
    acct = train.groupBy("account_id").agg(F.avg("amount").alias("acct_mean"),
                                           F.stddev_pop("amount").alias("acct_std"),
                                           F.count("*").alias("acct_n"),
                                           F.collect_set("location").alias("acct_locations"))
    dev = train.groupBy("device_id").agg(F.countDistinct("account_id").alias("device_accounts"))
    ip = train.groupBy("ip_address").agg(F.countDistinct("account_id").alias("ip_accounts"))
    return {"acct": acct, "device": dev, "ip": ip}


def bank_features(df: DataFrame, prof: dict[str, DataFrame], global_mean: float) -> DataFrame:
    """Butuh kolom: account_id, amount, tx_type, channel, location, device_id, ip_address."""
    out = (df.join(F.broadcast(prof["acct"]), "account_id", "left")
             .join(F.broadcast(prof["device"]), "device_id", "left")
             .join(F.broadcast(prof["ip"]), "ip_address", "left"))
    mean = F.coalesce(F.col("acct_mean"), F.lit(global_mean))
    std = F.when(F.coalesce(F.col("acct_std"), F.lit(0.0)) > 1, F.col("acct_std")).otherwise(F.lit(global_mean))
    return (out
            .withColumn("log_amount", F.log1p(F.col("amount")))
            .withColumn("amt_ratio", F.col("amount") / mean)
            .withColumn("amt_z", (F.col("amount") - mean) / std)
            .withColumn("is_debit", (F.col("tx_type") == "Debit").cast("double"))
            .withColumn("ch_atm", (F.col("channel") == "ATM").cast("double"))
            .withColumn("ch_online", (F.col("channel") == "Online").cast("double"))
            .withColumn("ch_branch", (F.col("channel") == "Branch").cast("double"))
            .withColumn("device_accounts", F.coalesce(F.col("device_accounts").cast("double"), F.lit(1.0)))
            .withColumn("ip_accounts", F.coalesce(F.col("ip_accounts").cast("double"), F.lit(1.0)))
            .withColumn("new_location", F.when(F.col("acct_locations").isNull(), F.lit(1.0))
                        .when(F.array_contains(F.col("acct_locations"), F.col("location")), F.lit(0.0))
                        .otherwise(F.lit(1.0)))
            .withColumn("acct_n", F.coalesce(F.col("acct_n").cast("double"), F.lit(0.0))))


def bank_reasons(df: DataFrame, dev_p95: float, ip_p95: float) -> DataFrame:
    return df.withColumn("reasons", F.concat_ws(", ",
        F.when(F.col("amt_ratio") >= 3, F.lit("JUMLAH_>3x_RATA2_AKUN")),
        F.when(F.col("amt_z") >= 3, F.lit("JUMLAH_JAUH_DI_ATAS_KEBIASAAN")),
        F.when(F.col("amt_ratio") <= 0.05, F.lit("JUMLAH_SANGAT_KECIL_DIBANDING_KEBIASAAN")),
        F.when(F.col("new_location") == 1, F.lit("LOKASI_BARU_UNTUK_AKUN")),
        F.when(F.col("device_accounts") > dev_p95, F.lit("PERANGKAT_DIPAKAI_BANYAK_AKUN")),
        F.when(F.col("ip_accounts") > ip_p95, F.lit("IP_DIPAKAI_BANYAK_AKUN")),
        F.when(F.col("acct_n") == 0, F.lit("AKUN_TANPA_RIWAYAT"))))


def kmeans_distance(df: DataFrame, centers: list[list[float]], vec_col: str = "scaled",
                    pred_col: str = "cluster") -> DataFrame:
    """Jarak Euclidean ke pusat cluster sendiri, dihitung dengan ekspresi SQL (tanpa Python UDF)."""
    from pyspark.ml.functions import vector_to_array
    lit_centers = F.array(*[F.array(*[F.lit(float(c)) for c in ctr]) for ctr in centers])
    arr = vector_to_array(F.col(vec_col))
    ctr = F.element_at(lit_centers, F.col(pred_col) + 1)
    sq = F.zip_with(arr, ctr, lambda x, y: (x - y) * (x - y))
    return df.withColumn("distance", F.sqrt(F.aggregate(sq, F.lit(0.0), lambda acc, v: acc + v)))
