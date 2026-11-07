"""
report_run.py - ringkasan hasil streaming dari PostgreSQL -> docs/results/streaming_run_<source>.json

    python scripts/report_run.py --source kafka
"""
import argparse
import json
import sys
import warnings
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from storage import db  # noqa: E402

warnings.filterwarnings("ignore")
ap = argparse.ArgumentParser()
ap.add_argument("--source", default="kafka", help="kafka | file (hanya untuk nama file hasil)")
a = ap.parse_args()

q = db.query
out = {
    "type": "FACTUAL RESULT - diukur dari tabel PostgreSQL setelah streaming",
    "ingestion": a.source, "generated_at": datetime.now().isoformat(timespec="seconds"),
    "tables": db.status(),
    "per_source_decision": q("SELECT source, decision, COUNT(*) n FROM predictions GROUP BY 1,2 ORDER BY 1,2")
    .to_dict("records"),
    "rejected_by_reason": q("SELECT reason, COUNT(*) n FROM rejected_records GROUP BY 1 ORDER BY 2 DESC").to_dict("records"),
    "duplicates_dropped_in_batch": int(q("SELECT COALESCE(SUM(rows_duplicate),0) n FROM batch_metrics").n[0]),
    "latency_ms": q("""SELECT ROUND(AVG(latency_ms)::numeric,1) mean,
                              ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::numeric,1) p50,
                              ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric,1) p95,
                              ROUND(MAX(latency_ms)::numeric,1) max FROM predictions""").iloc[0].to_dict(),
    "batch_processing_ms": q("""SELECT ROUND(AVG(processing_ms)::numeric,1) mean, ROUND(MAX(processing_ms)::numeric,1) max,
                                       COUNT(*) batches FROM batch_metrics""").iloc[0].to_dict(),
    "online_evaluation_vs_label": q("""SELECT source, COUNT(*) n, SUM(label) fraud,
            SUM((decision='BLOCK' AND label=1)::int) tp_block, SUM((decision='BLOCK' AND label=0)::int) fp_block,
            SUM((decision<>'APPROVE' AND label=1)::int) tp_review_or_block,
            SUM((decision<>'APPROVE' AND label=0)::int) fp_review_or_block
            FROM v_scored WHERE label IS NOT NULL GROUP BY source""").to_dict("records"),
}
out = json.loads(json.dumps(out, default=lambda o: float(o) if hasattr(o, "__float__") else str(o)))
f = config.RESULTS_DIR / f"streaming_run_{a.source}.json"
f.write_text(json.dumps(out, indent=2), encoding="utf-8")
print(json.dumps(out, indent=2))
