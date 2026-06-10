-- Enable pgcrypto for gen_random_uuid() on PostgreSQL < 13
-- On PostgreSQL 13+ gen_random_uuid() is built-in; this is a no-op if already installed.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- TABLE: transactions
-- ============================================================
CREATE TABLE transactions (
    id               UUID          NOT NULL DEFAULT gen_random_uuid(),
    transaction_id   VARCHAR(64)   NOT NULL,
    account_id       VARCHAR(64)   NOT NULL,
    merchant_id      VARCHAR(64)   NOT NULL,
    amount           NUMERIC(15,2) NOT NULL,
    currency         VARCHAR(3)    NOT NULL DEFAULT 'USD',
    country_code     VARCHAR(3)    NOT NULL,
    ip_address       VARCHAR(45),
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    fraud_status     VARCHAR(20)   NOT NULL DEFAULT 'APPROVED',
    fraud_score      INTEGER       NOT NULL DEFAULT 0,
    fraud_reason     VARCHAR(50)   NOT NULL DEFAULT 'NONE',
    rules_triggered  TEXT[],
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ,

    CONSTRAINT transactions_pkey             PRIMARY KEY (id),
    CONSTRAINT transactions_transaction_id_uk UNIQUE (transaction_id),
    CONSTRAINT transactions_amount_positive   CHECK (amount > 0),
    CONSTRAINT transactions_score_range       CHECK (fraud_score BETWEEN 0 AND 100)
);

-- ============================================================
-- TABLE: fraud_alerts
-- ============================================================
CREATE TABLE fraud_alerts (
    id                UUID        NOT NULL DEFAULT gen_random_uuid(),
    transaction_id    VARCHAR(64) NOT NULL,
    account_id        VARCHAR(64) NOT NULL,
    fraud_score       INTEGER     NOT NULL,
    fraud_reason      VARCHAR(50) NOT NULL,
    rules_triggered   TEXT[],
    alerted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged      BOOLEAN     NOT NULL DEFAULT FALSE,
    acknowledged_at   TIMESTAMPTZ,
    acknowledged_by   VARCHAR(128),

    CONSTRAINT fraud_alerts_pkey          PRIMARY KEY (id),
    CONSTRAINT fraud_alerts_txn_fk        FOREIGN KEY (transaction_id)
                                              REFERENCES transactions(transaction_id)
                                              ON DELETE RESTRICT
                                              ON UPDATE CASCADE,
    CONSTRAINT fraud_alerts_score_range   CHECK (fraud_score BETWEEN 0 AND 100),
    CONSTRAINT fraud_alerts_ack_coherence CHECK (
        (acknowledged = FALSE AND acknowledged_at IS NULL AND acknowledged_by IS NULL)
        OR
        (acknowledged = TRUE  AND acknowledged_at IS NOT NULL AND acknowledged_by IS NOT NULL)
    )
);

-- ============================================================
-- INDEXES: transactions
-- ============================================================
CREATE INDEX idx_transactions_account_id
    ON transactions (account_id);

CREATE INDEX idx_transactions_created_at
    ON transactions (created_at DESC);

CREATE INDEX idx_transactions_fraud_status
    ON transactions (fraud_status);

CREATE INDEX idx_transactions_account_created
    ON transactions (account_id, created_at DESC);

-- ============================================================
-- INDEXES: fraud_alerts
-- ============================================================
CREATE INDEX idx_fraud_alerts_account_id
    ON fraud_alerts (account_id);

CREATE INDEX idx_fraud_alerts_alerted_at
    ON fraud_alerts (alerted_at DESC);

CREATE INDEX idx_fraud_alerts_acknowledged
    ON fraud_alerts (acknowledged);
