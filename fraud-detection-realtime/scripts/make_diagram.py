"""make_diagram.py - menggambar diagram arsitektur -> docs/architecture.png"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "docs" / "architecture.png"
LAYERS = [
    ("1  DATA SOURCES (3 dataset · 881.962 baris)", "#2b6cb0", [
        "Transaksi Bank\nbank_transactions_data_2.csv\n2.512 baris · tanpa label",
        "Pembayaran Bank (BankSim)\nbs140513_032310.csv\n594.643 baris · label fraud",
        "Transaksi Kartu Kredit\ncreditcard.csv\n284.807 baris · label Class"]),
    ("2  DATA INGESTION · Apache Kafka", "#c05621", ["Python CSV Producer\n(pandas, per chunk)",
                                                     "Event JSON terpadu\nkey = account_id",
                                                     "Topic: transactions\n3 partisi (stream queue)"]),
    ("3  STREAM PROCESSING · Spark Structured Streaming", "#2c7a7b",
     ["Pembersihan\nnull · negatif · sumber salah · duplikat", "Transformasi\nfeature engineering per sumber",
      "Agregasi\nwindow 1 menit per sumber"]),
    ("4  DATA STORAGE · PostgreSQL", "#6b46c1", ["transactions · predictions", "alerts · agg_minute",
                                                 "rejected_records · batch_metrics"]),
    ("5  BIG DATA ANALYTICS · Spark MLlib", "#b83280", ["Clustering KMeans\nbank (skor anomali)",
                                                        "Klasifikasi GBT\nBankSim (prediksi fraud)",
                                                        "Klasifikasi GBT (+ LR)\nkartu kredit (prediksi fraud)"]),
    ("6  VISUALISASI · Streamlit (Power BI opsional)", "#975a16", ["KPI · grafik · tabel live",
                                                                    "Evaluasi langsung vs label",
                                                                    "Topic Kafka fraud_alerts"]),
    ("7  HASIL AKHIR", "#2f855a", ["Deteksi fraud\nskor risiko 0..1", "Peringatan dini\n(alert merah)",
                                   "Keputusan\nSETUJUI · TINJAU · BLOKIR"]),
]

fig, ax = plt.subplots(figsize=(12, 14.5))
ax.set_xlim(0, 12); ax.set_ylim(0, 15.2); ax.axis("off")
ax.text(6, 14.85, "Real-Time Fraud Detection System - Big Data Real-Time Data Pipeline", ha="center", fontsize=15,
        weight="bold", color="#1a202c")
h, gap, top = 1.62, 0.42, 14.3
for i, (title, col, boxes) in enumerate(LAYERS):
    y = top - (i + 1) * h - i * gap
    ax.add_patch(FancyBboxPatch((0.3, y), 11.4, h, boxstyle="round,pad=0.02,rounding_size=0.15", fc=col, ec=col,
                                alpha=0.12, lw=1.5))
    ax.text(0.55, y + h - 0.28, title, fontsize=11.5, weight="bold", color=col, va="center")
    for j, b in enumerate(boxes):
        x = 0.7 + j * 3.75
        ax.add_patch(FancyBboxPatch((x, y + 0.15), 3.35, h - 0.62, boxstyle="round,pad=0.02,rounding_size=0.1",
                                    fc="white", ec=col, lw=1.3))
        ax.text(x + 1.675, y + 0.15 + (h - 0.62) / 2, b, ha="center", va="center", fontsize=8.8, color="#1a202c")
        if i in (1, 2) and j < 2:
            ax.add_patch(FancyArrowPatch((x + 3.37, y + 0.65), (x + 3.73, y + 0.65), arrowstyle="-|>",
                                         mutation_scale=12, color=col))
    if i < len(LAYERS) - 1:
        ax.add_patch(FancyArrowPatch((6, y - 0.02), (6, y - gap + 0.02), arrowstyle="-|>", mutation_scale=18,
                                     color="#4a5568", lw=1.8))
ax.text(6, 0.12, "Docker Desktop: kafka · kafka-init · kafka-ui · postgres · spark-streaming · producer · dashboard "
        "(docker-compose.yml)", ha="center", fontsize=9, color="#4a5568", style="italic")
fig.savefig(OUT, dpi=140, bbox_inches="tight")
print("ok", OUT)
