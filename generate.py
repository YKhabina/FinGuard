"""
Финтех-пайплайн: генерация данных + Kafka (три независимых потока).

Команды:
    python main.py all                    # топики + генерация + отправка
    python main.py topics                 # только создать топики
    python main.py generate               # только сгенерировать JSONL
    python main.py produce                # только отправить в Kafka
    python main.py produce --only transactions --rate-tx 100
    python main.py all --rate-dev 2 --rate-auth 50 --rate-tx 200

Конфигурация — только из окружения (см. config.py и .env.example).
Никаких паролей и хостов в этом файле нет.
"""
import argparse
import json
import random
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ---------- Конфигурация из окружения ----------
from config import KAFKA_BOOTSTRAP, DATA_DIR


# ============================================================
# 0. Константы и настройки (без секретов)
# ============================================================
fake = Faker("ru_RU")
random.seed(42)
Faker.seed(42)

BOOTSTRAP = KAFKA_BOOTSTRAP
OUT_DIR   = DATA_DIR

# ---------- Объёмы ----------
N_CLIENTS        = 10_000
N_ACCOUNTS       = 12_000
N_CARDS          = 15_000
N_DEVICES        = 20_000
N_MERCHANTS      = 2_000
N_TRANSACTIONS   = 500_000
N_AUTHORIZATIONS = 300_000
N_DEVICE_EVENTS  = 100_000
FRAUD_RATE       = 0.04

# ---------- Домены ----------
CARD_TYPES    = ["debit", "credit", "virtual", "prepaid"]
PAY_SYSTEMS   = ["VISA", "MASTERCARD", "MIR"]
CARD_STATUSES = ["active", "blocked", "expired", "closed"]
DEVICE_TYPES  = ["mobile", "tablet", "desktop"]
DEVICE_OS     = ["iOS", "Android", "Windows", "macOS", "Linux"]
TX_TYPES      = ["purchase", "refund", "transfer", "withdrawal", "topup"]
TX_STATUSES   = ["approved", "declined", "pending", "reversed"]
AUTH_RESULTS  = ["approved", "declined", "3ds_challenge", "timeout"]
COUNTRIES     = ["RU", "KZ", "BY", "TR", "AE", "CN", "DE", "US"]
CITIES_BY_COUNTRY = {
    "RU": ["Москва", "Санкт-Петербург", "Казань", "Новосибирск", "Екатеринбург"],
    "KZ": ["Алматы", "Астана", "Шымкент"],
    "BY": ["Минск", "Гомель", "Брест"],
    "TR": ["Istanbul", "Ankara", "Izmir"],
    "AE": ["Dubai", "Abu Dhabi"],
    "CN": ["Beijing", "Shanghai", "Shenzhen"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "US": ["New York", "San Francisco", "Chicago"],
}
MCC_CODES = ["5411", "5812", "5541", "5912", "5999", "4814", "6011", "4111", "5732", "7995"]

START = datetime(2025, 1, 1, tzinfo=timezone.utc)
END   = datetime(2026, 12, 31, tzinfo=timezone.utc)

# ---------- Kafka ----------
TOPICS = {
    "transaction.events":   (6, 1),
    "authorization.events": (3, 1),
    "device.events":        (3, 1),
}
FILE_TO_TOPIC = {
    "transactions":   "transaction.events",
    "authorizations": "authorization.events",
    "device-events":  "device.events",
}
STREAM_ORDER = ["device-events", "authorizations", "transactions"]
KEY_FIELD = {
    "transaction.events":   "client_id",
    "authorization.events": "transaction_id",
    "device.events":        "device_id",
}
DEFAULT_RATES = {
    "device.events":        1.0,
    "authorization.events": 20.0,
    "transaction.events":   50.0,
}

shutdown = threading.Event()


def _sigint(*_):
    print("\n[SIGINT] stopping threads...", file=sys.stderr)
    shutdown.set()


# ============================================================
# 1. Утилиты
# ============================================================
def pid(prefix, n, width):
    return f"{prefix}-{n:0{width}d}"

def iso_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_date(dt):
    return dt.strftime("%Y-%m-%d")

def rand_dt():
    delta = END - START
    return START + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def dump_jsonl(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def key_for(topic, payload):
    return str(payload.get(KEY_FIELD[topic], ""))


# ============================================================
# 2. Справочники
# ============================================================
def gen_clients(n):
    used = set(); out = []
    for _ in range(n):
        while True:
            cid = pid("client", random.randint(1, 99_999), 5)
            if cid not in used:
                used.add(cid); break
        out.append({
            "client_id": cid,
            "created_at": iso_date(rand_dt()),
            "country": "RU",
            "city": random.choice(CITIES_BY_COUNTRY["RU"]),
            "risk_level": random.choices(["low","standard","high"], weights=[.7,.25,.05])[0],
            "kyc_status": random.choices(["pending","verified","rejected"], weights=[.1,.85,.05])[0],
        })
    return out


def gen_accounts(n, clients):
    ids = [c["client_id"] for c in clients]
    return [{
        "account_id": pid("account", random.randint(1, 999_999), 6),
        "client_id": random.choice(ids),
        "currency": "RUB",
        "opened_at": iso_date(rand_dt()),
    } for _ in range(n)]


def gen_cards(n, clients):
    ids = [c["client_id"] for c in clients]
    used = set(); out = []
    for _ in range(n):
        while True:
            cid = pid("card", random.randint(1, 99_999), 5)
            if cid not in used:
                used.add(cid); break
        issued = rand_dt()
        out.append({
            "card_id": cid,
            "client_id": random.choice(ids),
            "card_type": random.choice(CARD_TYPES),
            "payment_system": random.choice(PAY_SYSTEMS),
            "status": random.choices(CARD_STATUSES, weights=[.9,.06,.03,.01])[0],
            "issued_at": iso_date(issued),
            "expires_at": iso_date(issued + timedelta(days=365*random.randint(3,5))),
        })
    return out


def gen_devices(n, clients):
    ids = [c["client_id"] for c in clients]
    used = set(); out = []
    for _ in range(n):
        while True:
            did = pid("device", random.randint(1, 99_999), 5)
            if did not in used:
                used.add(did); break
        out.append({
            "device_id": did,
            "client_id": random.choice(ids),
            "device_type": random.choice(DEVICE_TYPES),
            "os": random.choice(DEVICE_OS),
            "trusted": random.random() < 0.8,
            "first_seen_at": iso_z(rand_dt()),
        })
    return out


def gen_merchants(n):
    used = set(); out = []
    for _ in range(n):
        while True:
            mid = pid("merchant", random.randint(1, 9_999), 4)
            if mid not in used:
                used.add(mid); break
        country = random.choice(COUNTRIES)
        out.append({
            "merchant_id": mid,
            "name": fake.company(),
            "mcc": random.choice(MCC_CODES),
            "country": country,
            "city": random.choice(CITIES_BY_COUNTRY[country]),
            "risk_level": random.choices(["low","standard","high"], weights=[.7,.25,.05])[0],
        })
    return out


# ============================================================
# 3. Профили и транзакции
# ============================================================
def build_profiles(clients, cards, devices):
    cards_by_cl, devices_by_cl = defaultdict(list), defaultdict(list)
    for c in cards:   cards_by_cl[c["client_id"]].append(c["card_id"])
    for d in devices: devices_by_cl[d["client_id"]].append(d["device_id"])

    profiles = {}
    for c in clients:
        cid = c["client_id"]
        if cid not in cards_by_cl or cid not in devices_by_cl:
            continue
        usual_countries = ["RU"]
        if random.random() < 0.10:
            usual_countries.append(random.choice(["TR", "AE", "KZ", "BY"]))
        usual_cities = [c["city"]]
        for cc in usual_countries[1:]:
            usual_cities.append(random.choice(CITIES_BY_COUNTRY[cc]))

        kind = random.choices(["day","evening","night_owl","mixed"],
                              weights=[.55,.25,.05,.15])[0]
        usual_hours = {"day":[8,22], "evening":[18,23],
                       "night_owl":[22,2], "mixed":[7,23]}[kind]

        avg_tx = max(200.0, min(round(random.lognormvariate(8.0, 0.8), 2), 25_000.0))
        tpd = random.choices([1,2,3,5,7,10,15,25],
                             weights=[.10,.18,.22,.20,.15,.08,.05,.02])[0]

        profiles[cid] = {
            "client_id": cid,
            "avg_transaction_amount": avg_tx,
            "usual_countries": usual_countries,
            "usual_cities": usual_cities,
            "usual_hours": usual_hours,
            "usual_devices": devices_by_cl[cid][: random.randint(1, len(devices_by_cl[cid]))],
            "usual_cards":   cards_by_cl[cid][: random.randint(1, len(cards_by_cl[cid]))],
            "transactions_per_day": tpd,
        }
    return profiles


def is_hour_usual(hour, usual_hours):
    s, e = usual_hours
    return s <= hour <= e if s <= e else (hour >= s or hour <= e)

def sample_usual_hour(usual_hours):
    s, e = usual_hours
    if s <= e: return random.randint(s, e)
    return random.randint(s, 23) if random.random() < 0.6 else random.randint(0, e)


def usual_transaction(client_id, profile, merchants_by_id, merchant_ids, tx_idx):
    hour = sample_usual_hour(profile["usual_hours"])
    delta_days = (END.date() - START.date()).days
    day = START + timedelta(days=random.randint(0, delta_days))
    dt = day.replace(hour=hour, minute=random.randint(0,59), second=random.randint(0,59))

    amount = max(50.0, round(random.gauss(profile["avg_transaction_amount"],
                                          profile["avg_transaction_amount"] * 0.6), 2))
    country = random.choice(profile["usual_countries"])
    if country == "RU":
        city = random.choice(CITIES_BY_COUNTRY["RU"]) if random.random() < 0.3 \
               else random.choice(profile["usual_cities"])
    else:
        city = random.choice(CITIES_BY_COUNTRY[country])

    merchant = merchants_by_id[random.choice(merchant_ids)]
    return {
        "transaction_id": pid("tx", tx_idx, 7),
        "client_id": client_id,
        "card_id": random.choice(profile["usual_cards"]),
        "merchant_id": merchant["merchant_id"],
        "amount": f"{amount:.2f}",
        "currency": "RUB",
        "transaction_type": random.choices(
            ["purchase","refund","transfer","withdrawal","topup"],
            weights=[.80,.04,.06,.05,.05])[0],
        "country": country, "city": city,
        "device_id": random.choice(profile["usual_devices"]),
        "event_datetime": iso_z(dt),
        "status": "approved",
    }


ANOMALY_TYPES = ["rapid_transactions","unusual_country","new_device","large_amount",
                 "night_activity","multiple_declines","cashout_pattern","account_takeover_pattern"]
ANOMALY_WEIGHTS = [0.15,0.15,0.12,0.12,0.10,0.10,0.13,0.13]


def _base_tx(client_id, profile, merchants_by_id, merchant_ids, tx_idx, dt,
             amount, country, city, device_id, status="approved",
             tx_type="purchase", card_id=None):
    merchant = merchants_by_id[random.choice(merchant_ids)]
    return {
        "transaction_id": pid("tx", tx_idx, 7),
        "client_id": client_id,
        "card_id": card_id or random.choice(profile["usual_cards"]),
        "merchant_id": merchant["merchant_id"],
        "amount": f"{amount:.2f}",
        "currency": "RUB",
        "transaction_type": tx_type,
        "country": country, "city": city,
        "device_id": device_id,
        "event_datetime": iso_z(dt),
        "status": status,
    }

def _unusual_dt(profile):
    base = rand_dt()
    if is_hour_usual(base.hour, profile["usual_hours"]):
        bad = [h for h in range(24) if not is_hour_usual(h, profile["usual_hours"])]
        base = base.replace(hour=random.choice(bad))
    return base

def _unusual_country(profile):
    return random.choice([c for c in COUNTRIES if c not in profile["usual_countries"]])


def gen_anomaly(client_id, profile, merchants_by_id, merchant_ids, new_device_pool, tx_idx):
    kind = random.choices(ANOMALY_TYPES, weights=ANOMALY_WEIGHTS)[0]
    base_dt = rand_dt(); txs = []

    if kind == "rapid_transactions":
        for k in range(random.randint(3, 6)):
            txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                                tx_idx+k, base_dt+timedelta(seconds=random.randint(30,180)*k),
                                amount=random.uniform(100, 5000),
                                country=random.choice(profile["usual_countries"]),
                                city=random.choice(profile["usual_cities"]),
                                device_id=random.choice(profile["usual_devices"])))
    elif kind == "unusual_country":
        c = _unusual_country(profile)
        txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids, tx_idx,
                            base_dt, random.uniform(2000, 30000), c,
                            random.choice(CITIES_BY_COUNTRY[c]),
                            random.choice(profile["usual_devices"])))
    elif kind == "new_device":
        txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids, tx_idx,
                            base_dt, random.uniform(1000, 20000),
                            random.choice(profile["usual_countries"]),
                            random.choice(profile["usual_cities"]),
                            random.choice(new_device_pool)))
    elif kind == "large_amount":
        txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids, tx_idx,
                            base_dt,
                            round(profile["avg_transaction_amount"]*random.uniform(10,30),2),
                            random.choice(profile["usual_countries"]),
                            random.choice(profile["usual_cities"]),
                            random.choice(profile["usual_devices"])))
    elif kind == "night_activity":
        txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids, tx_idx,
                            _unusual_dt(profile), random.uniform(500, 15000),
                            random.choice(profile["usual_countries"]),
                            random.choice(profile["usual_cities"]),
                            random.choice(profile["usual_devices"])))
    elif kind == "multiple_declines":
        for k in range(random.randint(3, 5)):
            txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                                tx_idx+k, base_dt+timedelta(minutes=k*2),
                                random.uniform(500, 5000),
                                random.choice(profile["usual_countries"]),
                                random.choice(profile["usual_cities"]),
                                random.choice(profile["usual_devices"]),
                                status="declined"))
    elif kind == "cashout_pattern":
        dt = _unusual_dt(profile)
        for k in range(random.randint(2, 4)):
            txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                                tx_idx+k, dt+timedelta(minutes=15*k),
                                random.uniform(20_000, 100_000),
                                random.choice(profile["usual_countries"]),
                                random.choice(profile["usual_cities"]),
                                random.choice(profile["usual_devices"]),
                                tx_type="withdrawal"))
    else:  # account_takeover_pattern
        dev = random.choice(new_device_pool)
        c = _unusual_country(profile); city = random.choice(CITIES_BY_COUNTRY[c])
        txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids, tx_idx,
                            base_dt, 500.0, c, city, dev, status="declined"))
        for k in range(1, random.randint(2, 4)):
            txs.append(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                                tx_idx+k, base_dt+timedelta(minutes=3*k),
                                round(profile["avg_transaction_amount"]*random.uniform(20,60),2),
                                c, city, dev))
    return txs, kind


def gen_transactions(profiles, merchants, devices, n_total):
    merchants_by_id = {m["merchant_id"]: m for m in merchants}
    merchant_ids = list(merchants_by_id.keys())
    all_devices = [d["device_id"] for d in devices]

    def new_device_for(profile):
        usual = set(profile["usual_devices"])
        return random.choice([d for d in all_devices if d not in usual])

    client_ids = list(profiles.keys())
    tx_rows, labels, batches = [], [], []
    normal = 0; idx = 1

    while len(tx_rows) < n_total:
        cid = random.choice(client_ids)
        profile = profiles[cid]
        if random.random() < FRAUD_RATE:
            txs, kind = gen_anomaly(cid, profile, merchants_by_id, merchant_ids,
                                    [new_device_for(profile)], idx)
            for t in txs:
                tx_rows.append(t)
                labels.append({"transaction_id": t["transaction_id"],
                               "client_id": cid, "fraud": True, "anomaly_type": kind})
            batches.append({"client_id": cid, "anomaly_type": kind,
                            "transaction_ids": [t["transaction_id"] for t in txs]})
            idx += len(txs)
        else:
            t = usual_transaction(cid, profile, merchants_by_id, merchant_ids, idx)
            tx_rows.append(t)
            labels.append({"transaction_id": t["transaction_id"],
                           "client_id": cid, "fraud": False, "anomaly_type": None})
            normal += 1; idx += 1

    tx_rows, labels = tx_rows[:n_total], labels[:n_total]
    print(f"  transactions: normal={normal}, "
          f"anomalies={len(tx_rows)-normal} ({(len(tx_rows)-normal)/len(tx_rows)*100:.2f}%)")
    return tx_rows, labels, batches


# ============================================================
# 4. Авторизации и device-events
# ============================================================
def gen_authorizations(n, tx_rows, devices):
    tx_by_id = {t["transaction_id"]: t for t in tx_rows}
    tx_ids = list(tx_by_id.keys())
    dev_by_client = defaultdict(list)
    for d in devices:
        dev_by_client[d["client_id"]].append(d["device_id"])

    out = []
    for i in range(n):
        tx = tx_by_id[random.choice(tx_ids)]
        device_id = tx.get("device_id") or random.choice(dev_by_client[tx["client_id"]])

        if tx["status"] == "approved":
            result = random.choices(["approved","3ds_challenge"], weights=[.93,.07])[0]
            code = "00"
        elif tx["status"] == "declined":
            result = random.choices(["declined","timeout"], weights=[.85,.15])[0]
            code = random.choice(["05","51","54","91"])
        else:
            result = random.choices(["approved","timeout","declined"], weights=[.6,.3,.1])[0]
            code = "00" if result == "approved" else "91"

        out.append({
            "authorization_id": pid("auth", i+1, 8),
            "transaction_id": tx["transaction_id"],
            "device_id": device_id,
            "result": result,
            "response_code": code,
            "risk_score": round(random.uniform(0, 1), 4),
            "is_3ds": random.random() < 0.6,
            "created_at": tx["event_datetime"],
        })
    return out


DEVICE_EVENT_TYPES = ["login","logout","token_refresh","device_added",
                      "device_trusted","device_blocked","push_opened"]

def gen_device_events(n, devices):
    out = []
    for i in range(n):
        d = random.choice(devices)
        ev = random.choices(DEVICE_EVENT_TYPES, weights=[.30,.20,.15,.08,.10,.02,.15])[0]
        out.append({
            "event_id": pid("dev-ev", i+1, 8),
            "device_id": d["device_id"],
            "client_id": d["client_id"],
            "event_type": ev,
            "os": d["os"],
            "ip": fake.ipv4_public() if random.random() < 0.9 else fake.ipv4(),
            "country": random.choices(["RU","KZ","BY","TR","AE","CN","DE","US"],
                                      weights=[.7,.05,.03,.07,.05,.03,.04,.03])[0],
            "event_datetime": iso_z(rand_dt()),
            "trusted": d["trusted"] if ev in ("login","token_refresh") else None,
        })
    return out


# ============================================================
# 5. Kafka: создание топиков
# ============================================================
def cmd_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [
        NewTopic(name=n, num_partitions=p, replication_factor=r,
                 config={"retention.ms": str(7*24*3600*1000),
                         "cleanup.policy": "delete"})
        for n, (p, r) in TOPICS.items() if n not in existing
    ]
    if not to_create:
        print("All topics exist."); return
    for name, fut in admin.create_topics(to_create).items():
        try:
            fut.result(); print(f"created: {name}")
        except Exception as e:
            print(f"failed: {name}: {e}")


# ============================================================
# 6. Генерация в JSONL
# ============================================================
def cmd_generate():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("clients...");         clients = gen_clients(N_CLIENTS)
    print("accounts...");        accounts = gen_accounts(N_ACCOUNTS, clients)
    print("cards...");           cards = gen_cards(N_CARDS, clients)
    print("devices...");         devices = gen_devices(N_DEVICES, clients)
    print("merchants...");       merchants = gen_merchants(N_MERCHANTS)
    print("profiles...");        profiles = build_profiles(clients, cards, devices)
    print("transactions...");    tx, labels, batches = gen_transactions(
        profiles, merchants, devices, N_TRANSACTIONS)
    print("authorizations...");  auths = gen_authorizations(N_AUTHORIZATIONS, tx, devices)
    print("device events...");   dev_ev = gen_device_events(N_DEVICE_EVENTS, devices)

    dump_jsonl(clients,   OUT_DIR / "clients.jsonl")
    dump_jsonl(accounts,  OUT_DIR / "accounts.jsonl")
    dump_jsonl(cards,     OUT_DIR / "cards.jsonl")
    dump_jsonl(devices,   OUT_DIR / "devices.jsonl")
    dump_jsonl(merchants, OUT_DIR / "merchants.jsonl")
    dump_jsonl(list(profiles.values()), OUT_DIR / "profiles.jsonl")

    dump_jsonl(tx,     OUT_DIR / "transactions.jsonl")
    dump_jsonl(auths,  OUT_DIR / "authorizations.jsonl")
    dump_jsonl(dev_ev, OUT_DIR / "device-events.jsonl")

    dump_jsonl(labels,  OUT_DIR / "labels.jsonl")
    dump_jsonl(batches, OUT_DIR / "anomaly_batches.jsonl")

    print(f"done: tx={len(tx)} auth={len(auths)} dev_ev={len(dev_ev)}")


# ============================================================
# 7. Kafka: три независимых потока-продюсера
# ============================================================
def _make_producer(client_id):
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": client_id,
        "linger.ms": 20,
        "batch.size": 32 * 1024,
        "compression.type": "snappy",
        "acks": "1",
        "enable.idempotence": True,
        "queue.buffering.max.messages": 200_000,
    })

def _delivery(err, msg):
    if err is not None:
        print(f"[{msg.topic()}] delivery ERROR partition={msg.partition()}: {err}",
              file=sys.stderr)

def _producer_thread(path: Path, topic: str, rate: float, limit):
    producer = _make_producer(f"producer-{topic}")
    interval = 1.0 / rate if rate > 0 else 0.0
    sent = 0; t0 = time.time()
    print(f"[{topic}] START rate={rate or 'max'} msg/s file={path.name}", file=sys.stderr)

    try:
        with path.open(encoding="utf-8") as f:
            for line in f:
                if shutdown.is_set(): break
                if not line.strip(): continue
                payload = json.loads(line)
                producer.produce(
                    topic=topic,
                    key=key_for(topic, payload).encode(),
                    value=json.dumps(payload, ensure_ascii=False).encode(),
                    callback=_delivery,
                )
                producer.poll(0)
                sent += 1
                if limit and sent >= limit: break
                if interval > 0: time.sleep(interval)
    except Exception as e:
        print(f"[{topic}] FATAL: {e}", file=sys.stderr)
    finally:
        producer.flush(10)
        dt = max(time.time() - t0, 1e-6)
        print(f"[{topic}] STOP: {sent} msgs in {dt:.1f}s ({sent/dt:.1f} msg/s)",
              file=sys.stderr)


def cmd_produce(args):
    rates = {
        "device.events":        args.rate_dev,
        "authorization.events": args.rate_auth,
        "transaction.events":   args.rate_tx,
    }
    only = set(args.only) if args.only else None
    threads = []

    for stem in STREAM_ORDER:
        if only and stem not in only:
            continue
        path = OUT_DIR / f"{stem}.jsonl"
        if not path.exists():
            print(f"[skip] {path} not found", file=sys.stderr); continue
        topic = FILE_TO_TOPIC[stem]
        t = threading.Thread(
            target=_producer_thread,
            args=(path, topic, rates[topic], args.limit),
            daemon=False,
            name=f"thr-{topic}",
        )
        t.start()
        threads.append((topic, t))

    try:
        for _, t in threads:
            while t.is_alive():
                t.join(0.5)
    except KeyboardInterrupt:
        shutdown.set()
        for _, t in threads:
            t.join(5)

    print("\nAll producer threads stopped.")


# ============================================================
# 8. CLI
# ============================================================
def main():
    ap = argparse.ArgumentParser(description="Финтех-пайплайн: генерация + Kafka")
    ap.add_argument("command", choices=["all", "topics", "generate", "produce"])
    ap.add_argument("--only", nargs="*", default=None,
                    help="device-events authorizations transactions")
    ap.add_argument("--rate-dev",  type=float, default=DEFAULT_RATES["device.events"])
    ap.add_argument("--rate-auth", type=float, default=DEFAULT_RATES["authorization.events"])
    ap.add_argument("--rate-tx",   type=float, default=DEFAULT_RATES["transaction.events"])
    ap.add_argument("--limit", type=int, default=None,
                    help="максимум сообщений на поток (для отладки)")
    args = ap.parse_args()

    if args.command == "topics":
        cmd_topics()
    elif args.command == "generate":
        cmd_generate()
    elif args.command == "produce":
        cmd_produce(args)
    else:  # all
        cmd_topics()
        cmd_generate()
        cmd_produce(args)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _sigint)
    main()