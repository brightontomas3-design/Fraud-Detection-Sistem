-- schema.sql - penyimpanan PostgreSQL untuk Real-Time Fraud Detection System
-- (dijalankan otomatis oleh container postgres saat pertama kali dibuat, dan oleh storage/db.py --init)

CREATE TABLE IF NOT EXISTS transactions (            -- transaksi yang lolos pembersihan
    event_id        VARCHAR(40) PRIMARY KEY,
    source          VARCHAR(10)  NOT NULL,            -- bank | card | banksim
    account_id      VARCHAR(40),
    counterparty    VARCHAR(40),
    amount          NUMERIC(16,2) NOT NULL,
    tx_type         VARCHAR(40),
    channel         VARCHAR(30),
    location        VARCHAR(60),
    device_id       VARCHAR(40),
    ip_address      VARCHAR(40),
    original_time   VARCHAR(40),                      -- waktu asli di CSV
    event_time      TIMESTAMP    NOT NULL,            -- waktu event (simulasi real-time, UTC)
    label           SMALLINT,                         -- label fraud asli (NULL = dataset tanpa label)
    ingested_at     TIMESTAMP    NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS ix_tx_time   ON transactions (event_time);
CREATE INDEX IF NOT EXISTS ix_tx_source ON transactions (source);
CREATE INDEX IF NOT EXISTS ix_tx_acct   ON transactions (account_id);

CREATE TABLE IF NOT EXISTS predictions (             -- hasil Spark MLlib untuk setiap transaksi
    event_id        VARCHAR(40) PRIMARY KEY REFERENCES transactions(event_id) ON DELETE CASCADE,
    source          VARCHAR(10)  NOT NULL,
    model           VARCHAR(40)  NOT NULL,
    risk_score      DOUBLE PRECISION NOT NULL,        -- 0..1
    decision        VARCHAR(10)  NOT NULL,            -- APPROVE | REVIEW | BLOCK
    reasons         TEXT,
    cluster         SMALLINT,                         -- hanya untuk model clustering (bank)
    latency_ms      DOUBLE PRECISION,                 -- dari producer kirim -> tersimpan
    processed_at    TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS ix_pred_decision ON predictions (decision);
CREATE INDEX IF NOT EXISTS ix_pred_time     ON predictions (processed_at);

CREATE TABLE IF NOT EXISTS alerts (                  -- peringatan dini (REVIEW / BLOCK)
    alert_id        BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(40) UNIQUE REFERENCES transactions(event_id) ON DELETE CASCADE,
    source          VARCHAR(10),
    account_id      VARCHAR(40),
    amount          NUMERIC(16,2),
    risk_score      DOUBLE PRECISION,
    decision        VARCHAR(10),
    reasons         TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS ix_alert_time ON alerts (created_at);

CREATE TABLE IF NOT EXISTS agg_minute (              -- agregasi Spark Streaming per menit per sumber
    window_start    TIMESTAMP   NOT NULL,
    window_end      TIMESTAMP   NOT NULL,
    source          VARCHAR(10) NOT NULL,
    tx_count        BIGINT,
    total_amount    NUMERIC(20,2),
    avg_amount      NUMERIC(16,2),
    max_amount      NUMERIC(16,2),
    known_fraud     BIGINT,
    updated_at      TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    PRIMARY KEY (window_start, source)
);

CREATE TABLE IF NOT EXISTS rejected_records (        -- data yang ditolak saat pembersihan
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(40),
    source          VARCHAR(10),
    reason          VARCHAR(60),
    payload         TEXT,
    rejected_at     TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS batch_metrics (           -- kesehatan pipeline per micro-batch
    id              BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT,
    rows_in         INT,
    rows_valid      INT,
    rows_rejected   INT,
    rows_duplicate  INT,
    n_review        INT,
    n_block         INT,
    processing_ms   DOUBLE PRECISION,
    avg_latency_ms  DOUBLE PRECISION,
    created_at      TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

-- view gabungan untuk dasbor (dan untuk Power BI)
CREATE OR REPLACE VIEW v_scored AS
SELECT t.*, p.model, p.risk_score, p.decision, p.reasons, p.cluster, p.latency_ms, p.processed_at
FROM transactions t JOIN predictions p USING (event_id);
