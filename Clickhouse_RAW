"""
ClickHouse RAW-слой: DDL + загрузка JSONL.

Режимы:
    python clickhouse_raw.py ddl                 # создать БД и таблицы
    python clickhouse_raw.py load                # залить generated/*.jsonl
    python clickhouse_raw.py load --truncate     # очистить перед загрузкой
    python clickhouse_raw.py load --file generated/transactions.jsonl
    python clickhouse_raw.py kafka-ddl           # Kafka Engine + MV (стриминг)

Принцип RAW: payload-поля — String, никаких CAST/Decimal.
amount="48 990,00" сохраняется как есть.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect

from config import CLICKHOUSE as CH, DATA_DIR


# ============================================================
# 0. Маппинг файл → таблица + колонки
# ============================================================
# (json_field, ch_column) — всё String, никаких приведений
SPECS = {
    "transactions": {
        "table": "raw_transactions",
        "columns": [
            ("transaction_id",   "transaction_id"),
            ("client_id",        "client_id"),
            ("card_id",          "card_id"),
            ("merchant_id",      "merchant_id"),
            ("amount",           "amount"),          # "48 990,00" — как есть
            ("currency",         "currency"),
            ("transaction_type", "transaction_type"),
            ("country",          "country"),
            ("city",             "city"),
            ("device_id",        "device_id"),
            ("event_datetime",   "event_datetime"),
            ("status",           "status"),
        ],
    },
    "authorizations": {
        "table": "raw_authorizations",
        "columns": [
            ("authorization_id", "authorization_id"),
            ("transaction_id",   "transaction_id"),
            ("device_id",        "device_id"),
            ("result",           "result"),
            ("response_code",    "response_code"),
            ("risk_score",       "risk_score"),      # String, не Float
            ("is_3ds",           "is_3ds"),          # String, не Bool
            ("created_at",       "created_at"),
        ],
    },
    "device-events": {
        "table": "raw_device_events",
        "columns": [
            ("event_id",       "event_id"),
            ("device_id",      "device_id"),
            ("client_id",      "client_id"),
            ("event_type",     "event_type"),
            ("os",             "os"),
            ("ip",             "ip"),
            ("country",        "country"),
            ("event_datetime", "event_datetime"),
            ("trusted",        "trusted"),           # String, не Bool
        ],
    },
}

# Метаданные ingestion, которые добавляются к каждой строке
META_COLUMNS = [
    "ingestion_datetime",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]


# ============================================================
# 1. Клиент
# ============================================================
def get_client():
    return clickhouse_connect.get_client(
        host=CH["host"],
        port=CH["port"],
        username=CH["user"],
        password=CH["password"],
        database=CH["database"],
    )

def get_client_no_db():
    """Для CREATE DATABASE нужен клиент без указания БД."""
    return clickhouse_connect.get_client(
        host=CH["host"],
        port=CH["port"],
        username=CH["user"],
        password=CH["password"],
    )


# ============================================================
# 2. DDL: база + три MergeTree таблицы
# ============================================================
def cmd_ddl():
    # 2.1 База
    client = get_client_no_db()
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH['database']}")
    client.close()

    client = get_client()

    # 2.2 raw_transactions
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH['database']}.raw_transactions
        (
            ingestion_datetime DateTime DEFAULT now(),
            kafka_topic        String,
            kafka_partition    UInt64,
            kafka_offset       UInt64,

            transaction_id    String,
            client_id         String,
            card_id           String,
            merchant_id       String,
            amount            String,
            currency          String,
            transaction_type  String,
            country           String,
            city              String,
            device_id         String,
            event_datetime    String,
            status            String
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(ingestion_datetime)
        ORDER BY (ingestion_datetime, kafka_topic, kafka_partition, kafka_offset)
    """)

    # 2.3 raw_authorizations
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH['database']}.raw_authorizations
        (
            ingestion_datetime DateTime DEFAULT now(),
            kafka_topic        String,
            kafka_partition    UInt64,
            kafka_offset       UInt64,

            authorization_id  String,
            transaction_id    String,
            device_id         String,
            result            String,
            response_code     String,
            risk_score        String,
            is_3ds            String,
            created_at        String
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(ingestion_datetime)
        ORDER BY (ingestion_datetime, kafka_topic, kafka_partition, kafka_offset)
    """)

    # 2.4 raw_device_events
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH['database']}.raw_device_events
        (
            ingestion_datetime DateTime DEFAULT now(),
            kafka_topic        String,
            kafka_partition    UInt64,
            kafka_offset       UInt64,

            event_id         String,
            device_id        String,
            client_id        String,
            event_type       String,
            os               String,
            ip               String,
            country          String,
            event_datetime   String,
            trusted          String
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(ingestion_datetime)
        ORDER BY (ingestion_datetime, kafka_topic, kafka_partition, kafka_offset)
    """)

    client.close()
    print(f"DDL done: {CH['database']}.raw_transactions / "
          f"raw_authorizations / raw_device_events")


# ============================================================
# 3. Загрузка JSONL
# ============================================================
def _jsonl_rows(path: Path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _topic_for(stem: str) -> str:
    # для совместимости с producer.py — логическое имя топика
    return {
        "transactions":   "transaction.events",
        "authorizations": "authorization.events",
        "device-events":  "device.events",
    }[stem]


def load_file(client, stem: str, path: Path, batch_size: int = 10_000) -> int:
    """
    Читает JSONL и вставляет в raw-таблицу.

    Метаданные Kafka для JSONL-загрузки синтезируются:
      kafka_topic     = логическое имя топика
      kafka_partition = 0
      kafka_offset    = сквозной счётчик строки (уникален в файле)
      ingestion_datetime = now() в момент вставки

    Для настоящего Kafka-стриминга используйте режим kafka-ddl
    (Kafka Engine + Materialized View).
    """
    spec = SPECS[stem]
    table = spec["table"]
    cols = spec["columns"]
    topic = _topic_for(stem)

    pg_cols = META_COLUMNS + [c for _, c in cols]
    all_columns = pg_cols

    total = 0
    offset = 0
    batch = []

    now = datetime.now(timezone.utc).replace(tzinfo=None)  # ClickHouse DateTime — без tz

    for rec in _jsonl_rows(path):
        # payload: значения как есть; отсутствующие → пустая строка
        payload_vals = ["" if rec.get(jf) is None else str(rec.get(jf)) for jf, _ in cols]
        row = [now, topic, 0, offset] + payload_vals
        batch.append(row)
        offset += 1

        if len(batch) >= batch_size:
            client.insert(table, batch, column_names=all_columns)
            total += len(batch)
            batch.clear()

    if batch:
        client.insert(table, batch, column_names=all_columns)
        total += len(batch)

    return total


def cmd_load(args):
    client = get_client()

    # при --truncate чистим все целевые таблицы
    if args.truncate:
        for spec in SPECS.values():
            client.command(f"TRUNCATE TABLE IF EXISTS {CH['database']}.{spec['table']}")
        print(f"truncated: {CH['database']}.*")

    # какие файлы грузим
    if args.file:
        path = Path(args.file)
        if not path.exists():
            print(f"ERROR: file not found: {path}", file=sys.stderr)
            sys.exit(1)
        stem = path.stem
        if stem not in SPECS:
            print(f"ERROR: unknown file stem '{stem}'. Ожидается один из: "
                  f"{', '.join(SPECS)}", file=sys.stderr)
            sys.exit(1)
        targets = [(stem, path)]
    else:
        targets = []
        for stem in SPECS:
            path = DATA_DIR / f"{stem}.jsonl"
            if path.exists():
                targets.append((stem, path))
            else:
                print(f"[skip] {path} not found", file=sys.stderr)

    total = 0
    for stem, path in targets:
        print(f"[load] {path} -> {CH['database']}.{SPECS[stem]['table']} ...")
        n = load_file(client, stem, path, batch_size=args.batch)
        print(f"  -> inserted={n}")
        total += n

    print(f"\nDONE: rows inserted = {total}")

    # итоговый отчёт
    print("\nRow counts:")
    for spec in SPECS.values():
        res = client.query(f"SELECT count() FROM {CH['database']}.{spec['table']}")
        print(f"  {spec['table']:<22} {res.result_rows[0][0]:>10}")

    client.close()


# ============================================================
# 4. (Опционально) Kafka Engine + MV для стриминга
# ============================================================
KAFKA_SETTINGS = {
    "transaction.events":   ("kafka_raw_transactions",   "mv_raw_transactions",
                             "raw_transactions"),
    "authorization.events": ("kafka_raw_authorizations", "mv_raw_authorizations",
                             "raw_authorizations"),
    "device.events":        ("kafka_raw_device_events",  "mv_raw_device_events",
                             "raw_device_events"),
}

def cmd_kafka_ddl(args):
    """
    Создаёт Kafka Engine tables + Materialized Views.
    Требует, чтобы raw_* таблицы уже существовали (выполните ddl).
    """
    client = get_client()
    db = CH["database"]

    for topic, (kafka_tbl, mv, target) in KAFKA_SETTINGS.items():
        stem = {
            "transaction.events":   "transactions",
            "authorization.events": "authorizations",
            "device.events":        "device-events",
        }[topic]
        cols = SPECS[stem]["columns"]
        cols_ddl = ",\n            ".join(f"{c} String" for _, c in cols)

        client.command(f"""
            CREATE TABLE IF NOT EXISTS {db}.{kafka_tbl}
            (
                {cols_ddl}
            )
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list    = '{args.bootstrap}',
                kafka_topic_list     = '{topic}',
                kafka_group_name     = 'clickhouse-{target}',
                kafka_format         = 'JSONEachRow',
                kafka_num_consumers  = 2,
                kafka_max_block_size = 65536,
                kafka_handle_error_mode = 'stream'
        """)

        select_cols = ",\n            ".join(c for _, c in cols)
        client.command(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.{mv}
            TO {db}.{target}
            AS
            SELECT
                now()        AS ingestion_datetime,
                _topic       AS kafka_topic,
                _partition   AS kafka_partition,
                _offset      AS kafka_offset,
                {select_cols}
            FROM {db}.{kafka_tbl}
        """)
        print(f"kafka-ddl: {db}.{kafka_tbl} -> {db}.{target}")

    client.close()
    print("Kafka Engine + MV созданы. Producer можно запускать.")


# ============================================================
# 5. CLI
# ============================================================
def main():
    ap = argparse.ArgumentParser(description="ClickHouse RAW: DDL + загрузка")
    sub = ap.add_subparsers(dest="command", required=True)

    sub.add_parser("ddl", help="создать базу и raw_* таблицы")

    p_load = sub.add_parser("load", help="залить generated/*.jsonl")
    p_load.add_argument("--file", default=None,
                        help="конкретный файл (transactions/authorizations/device-events)")
    p_load.add_argument("--truncate", action="store_true",
                        help="TRUNCATE перед загрузкой")
    p_load.add_argument("--batch", type=int, default=10_000)

    p_kafka = sub.add_parser("kafka-ddl", help="Kafka Engine + MV (стриминг)")
    p_kafka.add_argument("--bootstrap", default="localhost:9092")

    args = ap.parse_args()

    if args.command == "ddl":
        cmd_ddl()
    elif args.command == "load":
        cmd_load(args)
    elif args.command == "kafka-ddl":
        cmd_kafka_ddl(args)


if __name__ == "__main__":
    main()
