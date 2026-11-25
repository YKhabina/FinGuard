python
"""
Финтех-пайплайн: генерация данных + Kafka (три независимых потока).

Команды:
    python main.py all                    # топики + генерация + отправка
    python main.py topics                 # только создать топики
    python main.py generate               # только сгенерировать JSONL
    python main.py produce                # только отправить в Kafka
    python main.py produce --only transactions --rate-tx 100

Конфигурация — только из окружения (см. config.py и .env.example).
Никаких паролей и хостов в этом файле нет.
"""
import argparse
import json
import math
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

# ВАЖНО: это доля «инцидентов», а не транзакций.
# Инцидент = пачка из 1..15 транзакций. Фактическая доля транзакций
# получается ≈ FRAUD_RATE * avg_batch_size. При 1.2% и среднем размере
# пачки ~3.3 получаем ≈ 4% транзакций — как в задании.
FRAUD_RATE = 0.012

# ---------- Домены ----------
CARD_TYPES    = ["debit", "credit", "virtual", "prepaid"]
PAY_SYSTEMS   = ["VISA", "MASTERCARD", "MIR"]
CARD_STATUSES = ["active", "blocked", "expired", "closed"]
DEVICE_TYPES  = ["mobile", "tablet", "desktop"]
DEVICE_OS     = ["iOS", "Android", "Windows", "macOS", "Linux"]
TX_TYPES      = ["purchase", "refund", "transfer", "withdrawal", "topup"]
TX_STATUSES   = ["approved", "declined", "pending", "reversed"]
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
    print("\n[SIGINT/SIGTERM] stopping threads...", file=sys.stderr)
    shutdown.set()


# ============================================================
# 1. Утилиты
# ============================================================
def pid(prefix, n, width):
    return f"{prefix}-{n:0{width}d}"

def iso_z(dt):
    """Всегда UTC, всегда с 'Z'. Naive datetime считаем UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_date(dt):
    return dt.strftime("%Y-%m-%d")

def parse_iso(s):
    """'YYYY-MM-DDTHH:MM:SSZ' -> aware datetime."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

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

def _sample(lst, k):
    """Безопасная случайная выборка (не больше длины)."""
    k = max(1, min(k, len(lst)))
    return random.sample(lst, k=k)


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
    """FIX #1: уникальность account_id через used-множество."""
    ids = [c["client_id"] for c in clients]
    used = set(); out = []
    # гарантируем каждому клиенту минимум один счёт
    for cid in ids:
        while True:
            aid = pid("account", random.randint(1, 999_999), 6)
            if aid not in used:
                used.add(aid); break
        out.append({
            "account_id": aid,
            "client_id": cid,
            "currency": "RUB",
            "opened_at": iso_date(rand_dt()),
        })
    # остальные счета — случайным клиентам
    while len(out) < n:
        while True:
            aid = pid("account", random.randint(1, 999_999), 6)
            if aid not in used:
                used.add(aid); break
        out.append({
            "account_id": aid,
            "client_id": random.choice(ids),
            "currency": "RUB",
            "opened_at": iso_date(rand_dt()),
        })
    return out


def _make_card(cid, used):
    while True:
        card_id = pid("card", random.randint(1, 99_999), 5)
        if card_id not in used:
            used.add(card_id); break
    issued = rand_dt()
    return {
        "card_id": card_id,
        "client_id": cid,
        "card_type": random.choice(CARD_TYPES),
        "payment_system": random.choice(PAY_SYSTEMS),
        "status": random.choices(CARD_STATUSES, weights=[.9,.06,.03,.01])[0],
        "issued_at": iso_date(issued),
        "expires_at": iso_date(issued + timedelta(days=365*random.randint(3,5))),
    }


def gen_cards(n, clients):
    """FIX #2: каждому клиенту гарантированно ≥1 карта."""
    ids = [c["client_id"] for c in clients]
    used = set(); out = []
    for cid in ids:
        out.append(_make_card(cid, used))
    while len(out) < n:
        out.append(_make_card(random.choice(ids), used))
    return out


def _make_device(cid, used):
    while True:
        did = pid("device", random.randint(1, 99_999), 5)
        if did not in used:
            used.add(did); break
    return {
        "device_id": did,
        "client_id": cid,
        "device_type": random.choice(DEVICE_TYPES),
        "os": random.choice(DEVICE_OS),
        "trusted": random.random() < 0.8,
        "first_seen_at": iso_z(rand_dt()),
    }


def gen_devices(n, clients):
    """FIX #2: каждому клиенту гарантированно ≥1 устройство."""
    ids = [c["client_id"] for c in clients]
    used = set(); out = []
    for cid in ids:
        out.append(_make_device(cid, used))
    while len(out) < n:
        out.append(_make_device(random.choice(ids), used))
    return out


def gen_merchants(n):
    used = set(); out = []
    for _ in range(n):
        while True:
            mid = pid("merchant", random.randint(1, 99_999), 5)
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
    """FIX #3: random.sample вместо [:k] — «обычные» карты/устройства
    не привязаны к порядку появления в списке."""
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
            "usual_devices": _sample(devices_by_cl[cid],
                                     random.randint(1, len(devices_by_cl[cid]))),
            "usual_cards":   _sample(cards_by_cl[cid],
                                     random.randint(1, len(cards_by_cl[cid]))),
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


def _lognormal_amount(avg):
    """FIX #4: логнормальное распределение с заданным средним.
    Нет «прилипания» к 50 ₽, как у gauss."""
    # для lognormal: median = exp(mu), mean = exp(mu + sigma^2/2)
    # фиксируем sigma=0.6, подбираем mu так, чтобы mean == avg
    sigma = 0.6
    mu = math.log(max(avg, 1.0)) - 0.5 * sigma * sigma
    return round(random.lognormvariate(mu, sigma), 2)


def usual_transaction(client_id, profile, merchants_by_id, merchant_ids, tx_idx):
    hour = sample_usual_hour(profile["usual_hours"])
    delta_days = (END.date() - START.date()).days
    day = START + timedelta(days=random.randint(0, delta_days))
    dt = day.replace(hour=hour, minute=random.randint(0,59), second=random.randint(0,59))

    amount = max(50.0, _lognormal_amount(profile["avg_transaction_amount"]))
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


ANOMALY_TYPES = [
    "rapid_transactions",
    "unusual_country",
    "new_device",
    "large_amount",
    "night_activity",
    "card_testing",               # FIX #26: было multiple_declines
    "cashout_pattern",
    "account_takeover_pattern",
]
ANOMALY_WEIGHTS = [0.15, 0.13, 0.12, 0.12, 0.10, 0.12, 0.13, 0.13]


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

def _unusual_country(profile):
    return random.choice([c for c in COUNTRIES if c not in profile["usual_countries"]])


def gen_anomaly(client_id, profile, merchants_by_id, merchant_ids,
                new_device_pool, tx_idx):
    """
    Возвращает (txs, next_idx, kind).
    FIX #9: next_idx явно, чтобы не полагаться на len(txs) у вызывающего.
    """
    kind = random.choices(ANOMALY_TYPES, weights=ANOMALY_WEIGHTS)[0]
    base_dt = rand_dt()
    txs = []
    idx = tx_idx

    def add(tx):
        nonlocal idx
        txs.append(tx)
        idx += 1

    if kind == "rapid_transactions":
        # FIX #24: 11–15 операций за <5 минут — реально триггерит Velocity
        n = random.randint(11, 15)
        for k in range(n):
            dt = base_dt + timedelta(seconds=random.randint(1, 25) * k)
            add(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                         idx, dt, amount=random.uniform(100, 5000),
                         country=random.choice(profile["usual_countries"]),
                         city=random.choice(profile["usual_cities"]),
                         device_id=random.choice(profile["usual_devices"])))

    elif kind == "unusual_country":
        c = _unusual_country(profile)
        add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                     base_dt, random.uniform(2000, 30000), c,
                     random.choice(CITIES_BY_COUNTRY[c]),
                     random.choice(profile["usual_devices"])))

    elif kind == "new_device":
        add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                     base_dt, random.uniform(1000, 20000),
                     random.choice(profile["usual_countries"]),
                     random.choice(profile["usual_cities"]),
                     random.choice(new_device_pool)))

    elif kind == "large_amount":
        add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                     base_dt,
                     round(profile["avg_transaction_amount"] * random.uniform(10, 30), 2),
                     random.choice(profile["usual_countries"]),
                     random.choice(profile["usual_cities"]),
                     random.choice(profile["usual_devices"])))

    elif kind == "night_activity":
        # FIX #13: фиксированное ночное окно 02:00–04:59, а не «нетипичное для профиля»
        dt = base_dt.replace(hour=random.choice([2, 3, 4]),
                             minute=random.randint(0, 59),
                             second=random.randint(0, 59))
        add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                     dt, random.uniform(500, 15000),
                     random.choice(profile["usual_countries"]),
                     random.choice(profile["usual_cities"]),
                     random.choice(profile["usual_devices"])))

    elif kind == "card_testing":
        # FIX #26: 3–5 мелких declined → 1 крупный approved
        n_decl = random.randint(3, 5)
        for k in range(n_decl):
            add(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                         idx, base_dt + timedelta(seconds=random.randint(30, 90) * k),
                         amount=random.uniform(50, 500),
                         country=random.choice(profile["usual_countries"]),
                         city=random.choice(profile["usual_cities"]),
                         device_id=random.choice(profile["usual_devices"]),
                         status="declined"))
        add(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                     idx, base_dt + timedelta(seconds=random.randint(400, 600)),
                     amount=random.uniform(50_000, 100_000),
                     country=random.choice(profile["usual_countries"]),
                     city=random.choice(profile["usual_cities"]),
                     device_id=random.choice(profile["usual_devices"]),
                     status="approved"))

    elif kind == "cashout_pattern":
        # FIX #14: нетипичная локация + новое устройство + серия снятий
        c = _unusual_country(profile)
        city = random.choice(CITIES_BY_COUNTRY[c])
        dev = random.choice(new_device_pool)
        for k in range(random.randint(2, 4)):
            add(_base_tx(client_id, profile, merchants_by_id, merchant_ids,
                         idx, base_dt + timedelta(minutes=15 * k),
                         amount=random.uniform(20_000, 100_000),
                         country=c, city=city, device_id=dev,
                         tx_type="withdrawal", status="approved"))

    else:  # account_takeover_pattern
        # FIX #25: обычные → атака с нового устройства в чужой стране → крупные суммы
        # 1) две обычные транзакции до атаки
        for k in range(2):
            add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                         base_dt + timedelta(minutes=random.randint(1, 5) * k),
                         amount=random.uniform(100, 3000),
                         country=random.choice(profile["usual_countries"]),
                         city=random.choice(profile["usual_cities"]),
                         device_id=random.choice(profile["usual_devices"])))
        # 2) «вход» с нового устройства в нетипичной стране
        attack_dt = base_dt + timedelta(minutes=random.randint(8, 15))
        c = _unusual_country(profile)
        city = random.choice(CITIES_BY_COUNTRY[c])
        dev = random.choice(new_device_pool)
        add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                     attack_dt, amount=random.uniform(100, 500),
                     country=c, city=city, device_id=dev, status="declined"))
        # 3) крупные операции
        for k in range(1, random.randint(3, 5)):
            add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                         attack_dt + timedelta(minutes=random.randint(2, 5) * k),
                         amount=round(profile["avg_transaction_amount"]
                                      * random.uniform(20, 60), 2),
                         country=c, city=city, device_id=dev,
                         status="approved"))
        # 4) финальный перевод
        add(_base_tx(client_id, profile, merchants_by_id, merchant_ids, idx,
                     attack_dt + timedelta(minutes=random.randint(15, 25)),
                     amount=random.uniform(80_000, 200_000),
                     country=c, city=city, device_id=dev,
                     tx_type="transfer", status="approved"))

    return txs, idx, kind


def gen_transactions(profiles, merchants, devices, n_total):
    # FIX #8: проверка на пустоту
    if not profiles:
        raise RuntimeError("profiles пуст — проверьте генерацию карт/устройств")

    merchants_by_id = {m["merchant_id"]: m for m in merchants}
    merchant_ids = list(merchants_by_id.keys())
    all_devices = [d["device_id"] for d in devices]

    def new_device_for(profile):
        usual = set(profile["usual_devices"])
        pool = [d for d in all_devices if d not in usual]
        if not pool:
            return random.choice(all_devices)
        return random.choice(pool)

    client_ids = list(profiles.keys())
    tx_rows, labels, batches = [], [], []
    normal = 0
    idx = 1

    while len(tx_rows) < n_total:
        cid = random.choice(client_ids)
        profile = profiles[cid]
        if random.random() < FRAUD_RATE:
            txs, idx, kind = gen_anomaly(
                cid, profile, merchants_by_id, merchant_ids,
                [new_device_for(profile)], idx,
            )
            for t in txs:
                tx_rows.append(t)
                labels.append({"transaction_id": t["transaction_id"],
                               "client_id": cid, "fraud": True, "anomaly_type": kind})
            batches.append({"client_id": cid, "anomaly_type": kind,
                            "transaction_ids": [t["transaction_id"] for t in txs]})
        else:
            t = usual_transaction(cid, profile, merchants_by_id, merchant_ids, idx)
            tx_rows.append(t)
            labels.append({"transaction_id": t["transaction_id"],
                           "client_id": cid, "fraud": False, "anomaly_type": None})
            normal += 1
            idx += 1

    tx_rows, labels = tx_rows[:n_total], labels[:n_total]
    # пересчитываем фактическую долю по обрезанному массиву
    fraud_tx = sum(1 for l in labels if l["fraud"])
    print(f"  transactions: normal={len(labels)-fraud_tx}, "
          f"anomalies={fraud_tx} ({fraud_tx/len(labels)*100:.2f}%)")
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

        # FIX #6: risk_score согласован со статусом
        if tx["status"] == "approved":
            result = random.choices(["approved","3ds_challenge"], weights=[.93,.07])[0]
            code = "00"
            risk = round(random.betavariate(2, 8), 4)       # смещено к 0
        elif tx["status"] == "declined":
            result = random.choices(["declined","timeout"], weights=[.85,.15])[0]
            code = random.choice(["05","51","54","91"])
            risk = round(random.betavariate(8, 2), 4)       # смещено к 1
        else:
            result = random.choices(["approved","timeout","declined"], weights=[.6,.3,.1])[0]
            code = "00" if result == "approved" else "91"
            risk = round(random.betavariate(5, 5), 4)

        # FIX #16: timeout/3ds_challenge — авторизация позже транзакции
        tx_dt = parse_iso(tx["event_datetime"])
        if result == "timeout":
            auth_dt = tx_dt + timedelta(seconds=random.randint(30, 120))
        elif result == "3ds_challenge":
            auth_dt = tx_dt + timedelta(seconds=random.randint(5, 30))
        else:
            auth_dt = tx_dt

        out.append({
            "authorization_id": pid("auth", i+1, 8),
            "transaction_id": tx["transaction_id"],
            "device_id": device_id,
            "result": result,
            "response_code": code,
            "risk_score": risk,
            "is_3ds": random.random() < 0.6,
            "created_at": iso_z(auth_dt),
        })
    return out


DEVICE_EVENT_TYPES = ["login","logout","token_refresh","device_added",
                      "device_trusted","device_blocked","push_opened"]

# FIX #7: пул «правдоподобных» IP на страну (для согласованности country ↔ ip)
IP_POOLS = {
    "RU": ["95.24.", "178.176.", "213.180.", "77.88."],
    "KZ": ["2.72.",   "5.34.",    "92.46."],
    "BY": ["93.84.",  "178.120.", "37.17."],
    "TR": ["88.255.", "178.233.", "195.87."],
    "AE": ["94.200.", "217.164.", "185.93."],
    "CN": ["114.114.", "202.108.", "221.179."],
    "DE": ["85.214.", "46.4.",    "78.46."],
    "US": ["8.8.",    "66.102.",  "23.22."],
}

def _ip_for_country(country):
    prefix = random.choice(IP_POOLS[country])
    return f"{prefix}{random.randint(1,254)}.{random.randint(1,254)}"


def gen_device_events(n, devices):
    out = []
    for i in range(n):
        d = random.choice(devices)
        ev = random.choices(DEVICE_EVENT_TYPES, weights=[.30,.20,.15,.08,.10,.02,.15])[0]
        country = random.choices(COUNTRIES,
                                 weights=[.7,.05,.03,.07,.05,.03,.04,.03])[0]
        ip = _ip_for_country(country)

        # FIX #23: событие не раньше first_seen_at
        first_seen = parse_iso(d["first_seen_at"])
        # случайное время между first_seen и END
        dt = first_seen + timedelta(seconds=random.randint(
            0, max(1, int((END - first_seen).total_seconds()))))

        # FIX #15: корректный trusted для device_trusted / device_blocked
        if ev == "device_trusted":
            trusted = True
        elif ev == "device_blocked":
            trusted = False
        elif ev in ("login", "token_refresh"):
            trusted = d["trusted"]
        else:
            trusted = None

        out.append({
            "event_id": pid("dev-ev", i+1, 8),
            "device_id": d["device_id"],
            "client_id": d["client_id"],
            "event_type": ev,
            "os": d["os"],
            "ip": ip,
            "country": country,
            "event_datetime": iso_z(dt),
            "trusted": trusted,
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
    for
