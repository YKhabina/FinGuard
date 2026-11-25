-- ============================================================
-- Карантин: quarantine
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.quarantine
(
    transaction_id    String,
    raw_payload       String,              -- исходный JSON целиком
    quarantine_reason Array(String),       -- несколько причин в одной строке
    quarantined_at    DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(quarantined_at)
ORDER BY (quarantined_at, transaction_id);

-- «Развёрнутые» причины — для аналитики «сколько упало по каждой причине»
CREATE TABLE IF NOT EXISTS raw.quarantine_reasons
(
    transaction_id String,
    reason         LowCardinality(String),
    quarantined_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(quarantined_at)
ORDER BY (reason, quarantined_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_quarantine_reasons
TO raw.quarantine_reasons
AS
SELECT
    transaction_id,
    arrayJoin(quarantine_reason) AS reason,
    quarantined_at
FROM raw.quarantine;
===================================================================
      INSERT
===================================================================
INSERT INTO raw.quarantine
    (transaction_id, raw_payload, quarantine_reason, quarantined_at)
SELECT
    transaction_id,
    raw_payload,
    arrayFilter(x -> x != '', [
        if(r_duplicate,          'duplicate_transaction_id', ''),
        if(r_unknown_client,     'unknown_client',           ''),
        if(r_unknown_card,       'unknown_card',             ''),
        if(r_card_other_client,  'card_belongs_to_other_client', ''),
        if(r_negative_amount,    'negative_amount',          ''),
        if(r_zero_amount,        'zero_amount',              ''),
        if(r_unknown_currency,   'unknown_currency',         ''),
        if(r_bad_timestamp,      'invalid_timestamp',        ''),
        if(r_before_card_issue,  'before_card_issue',        ''),
        if(r_after_card_expiry,  'after_card_expiry',        ''),
        if(r_unknown_merchant,   'unknown_merchant',         ''),
        if(r_missing_device,     'missing_device_id',        '')
    ]) AS quarantine_reason,
    now() AS quarantined_at
FROM flagged
WHERE
    r_duplicate OR r_unknown_client OR r_unknown_card OR r_card_other_client
 OR r_negative_amount OR r_zero_amount OR r_unknown_currency OR r_bad_timestamp
 OR r_before_card_issue OR r_after_card_expiry OR r_unknown_merchant OR r_missing_device;
