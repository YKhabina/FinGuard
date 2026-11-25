python
"""
DQ-пайплайн: RAW → clean + quarantine (ClickHouse).

12 правил:
    duplicate_transaction_id, unknown_client, unknown_card,
    card_belongs_to_other_client, negative_amount, zero_amount,
    unknown_currency, invalid_timestamp, before_card_issue,
    after_card_expiry, unknown_merchant, missing_device_id

Одна транзакция может иметь несколько причин → Array(String).

Запуск:
    python dq_pipeline.py run
    python dq_pipeline.py run --dry-run
    python dq_pipeline.py summary
"""
import argparse
import sys

import clickhouse_connect

from config import CLICKHOUSE as CH


# ============================================================
# CTE с проверками — один источник истины для всех INSERT/SELECT
# ============================================================
DQ_CTE = """
WITH
parsed AS (
    SELECT
        transaction_id, client_id, card_id, merchant_id, device_id,
        currency, status, transaction_type, country, city,
        toDecimal128OrNull(
            replaceAll(replaceAll(replaceAll(amount, ' ', ''), ',', '.'), chr(160), ''),
            2
        ) AS amount_num,
        parseDateTimeBestEffortOrNull(event_datetime) AS event_dt,
        raw_payload, kafka_topic, kafka_partition, kafka_offset, ingestion_datetime
    FROM raw.raw_transactions
),
enriched AS (
    SELECT p.*,
           c.client_id   AS known_client,
           cd.client_id  AS card_owner,
           cd.issued_at  AS card_issued,
           cd.expires_at AS card_expires,
           m.merchant_id AS known_merchant,
           d.device_id   AS known_device,
           cur.code      AS known_currency
    FROM parsed p
    LEFT JOIN core.clients    c   ON c.client_id   = p.client_id
    LEFT JOIN core.cards      cd  ON cd.card_id    = p.card_id
    LEFT JOIN core.merchants  m   ON m.merchant_id = p.merchant_id
    LEFT JOIN core.devices    d   ON d.device_id   = p.device_id
    LEFT JOIN core.currencies cur ON cur.code      = upper(p.currency)
),
dups AS (
    SELECT transaction_id FROM raw.raw_transactions
    GROUP BY transaction_id HAVING count() > 1
),
flagged AS (
    SELECT e.*,
        (e.transaction_id IN (SELECT transaction_id FROM dups))     AS r_duplicate,
        (e.known_client IS NULL)                                     AS r_unknown_client,
        (e.card_id <> '' AND e.card_owner IS NULL)                   AS r_unknown_card,
        (e.card_owner IS NOT NULL AND e.card_owner <> e.client_id)   AS r_card_other_client,
        (e.amount_num IS NOT NULL AND e.amount_num < 0)              AS r_negative_amount,
        (e.amount_num IS NOT NULL AND e.amount_num = 0)              AS r_zero_amount,
        (e.known_currency IS NULL)                                   AS r_unknown_currency,
        (e.event_dt IS NULL)                                         AS r_bad_timestamp,
        (e.event_dt IS NOT NULL AND e.card_issued IS NOT NULL
            AND toDate(e.event_dt) < e.card_issued)                  AS r_before_card_issue,
        (e.event_dt IS NOT NULL AND e.card_expires IS NOT NULL
            AND toDate(e.event_dt) > e.card_expires)                 AS r_after_card_expiry,
        (e.known_merchant IS NULL)                                   AS r_unknown_merchant,
        (e.device_id = '' OR e.device_id IS NULL)                    AS r_missing_device
    FROM enriched e
)
"""

ANY_BAD = """
(
    r_duplicate OR r_unknown_client OR r_unknown_card OR r_card_other_client
 OR r_negative_amount OR r_zero_amount OR r_unknown_currency OR r_bad_timestamp
 OR r_before_card_issue OR r_after_card_expiry OR r_unknown_merchant OR r_missing_device
)
"""

REASONS_ARRAY = """
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
])
"""

INSERT_QUARANTINE = DQ_CTE + f"""
INSERT INTO raw.quarantine
    (transaction_id, raw_payload, quarantine_reason, quarantined_at)
SELECT
    transaction_id,
    raw_payload,
    {REASONS_ARRAY} AS quarantine_reason,
    now() AS quarantined_at
FROM flagged
WHERE {ANY_BAD}
"""

INSERT_CLEAN = DQ_CTE + f"""
INSERT INTO clean.transactions
    (transaction_id, client_id, account_id, card_id, merchant_id, device_id,
     amount, currency, transaction_type, country, city,
     event_datetime, status,
     ingestion_datetime, kafka_topic, kafka_partition, kafka_offset)
SELECT
    transaction_id, client_id, '' AS account_id, card_id, merchant_id, device_id,
    amount_num, upper(currency), transaction_type, country, city,
    event_dt, status,
    ingestion_datetime, kafka_topic, kafka_partition, kafka_offset
FROM flagged
WHERE NOT {ANY_BAD}
"""

SUMMARY = DQ_CTE + f"""
SELECT {REASONS_ARRAY} AS reasons, count() AS cnt
FROM flagged
WHERE {ANY_BAD}
GROUP BY reasons
ORDER BY cnt DESC
"""


# ============================================================
# CLI
# ============================================================
def get_client():
    return clickhouse_connect.get_client(
        host=CH["host"], port=CH["port"],
        username=CH["user"], password=CH["password"],
        database=CH["database"],
    )


def cmd_summary():
    client = get_client()
    res = client.query(SUMMARY)
    print(f"{'reasons':<70} count")
    print("-" * 80)
    for reasons, cnt in res.result_rows:
        print(f"{','.join(reasons):<70} {cnt}")
    client.close()


def cmd_run(dry_run):
    client = get_client()
    if dry_run:
        bad = client.query(DQ_CTE + f"SELECT count() FROM flagged WHERE {ANY_BAD}").result_rows[0][0]
        good = client.query(DQ_CTE + f"SELECT count() FROM flagged WHERE NOT {ANY_BAD}").result_rows[0][0]
        print(f"[dry-run] valid={good}, quarantined={bad}")
        client.close()
        return

    client.command(INSERT_QUARANTINE)
    client.command(INSERT_CLEAN)
    client.command("OPTIMIZE TABLE clean.transactions FINAL")  # дедуп ReplacingMergeTree

    print("DQ run complete.")
    q = client.query("SELECT count() FROM raw.quarantine").result_rows[0][0]
    c = client.query("SELECT count() FROM clean.transactions").result_rows[0][0]
    print(f"  quarantine: {q}")
    print(f"  clean.transactions:      {c}")
    client.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["run", "summary"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.command == "run":
        cmd_run(args.dry_run)
    else:
        cmd_summary()


if __name__ == "__main__":
    main()
