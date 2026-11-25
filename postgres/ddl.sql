-- ============================================================
-- 0. Окружение (опционально)
-- ============================================================
-- CREATE DATABASE fintech;
-- \c fintech;

CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core, public;

-- Удобно для TIMESTAMPTZ-полей: всё в UTC
SET TIME ZONE 'UTC';


-- ============================================================
-- 1. CLIENT
-- ============================================================
CREATE TABLE clients (
    client_id      TEXT         PRIMARY KEY
                                CHECK (client_id ~ '^client-[0-9]{5}$'),
    created_at     DATE         NOT NULL DEFAULT CURRENT_DATE,
    country        CHAR(2)      NOT NULL,
    city           TEXT         NOT NULL,
    risk_level     TEXT         NOT NULL
                                CHECK (risk_level IN ('low','standard','high')),
    kyc_status     TEXT         NOT NULL
                                CHECK (kyc_status IN ('pending','verified','rejected')),

    -- защита от «пустых» значений
    CONSTRAINT clients_city_not_empty CHECK (length(trim(city)) > 0)
);

COMMENT ON TABLE  clients IS 'Клиенты (мастер-данные)';
COMMENT ON COLUMN clients.kyc_status IS 'pending / verified / rejected';

-- Индексы под типовые выборки
CREATE INDEX idx_clients_kyc_status   ON clients (kyc_status);
CREATE INDEX idx_clients_risk_level   ON clients (risk_level);
CREATE INDEX idx_clients_country_city ON clients (country, city);
-- частичный индекс: «неподтверждённые» клиенты — самый частый кейс для запроса ниже
CREATE INDEX idx_clients_kyc_unverified
    ON clients (client_id)
    WHERE kyc_status <> 'verified';


-- ============================================================
-- 2. ACCOUNT
-- ============================================================
CREATE TABLE accounts (
    account_id     TEXT         PRIMARY KEY
                                CHECK (account_id ~ '^account-[0-9]{6}$'),
    client_id      TEXT         NOT NULL
                                REFERENCES clients (client_id)
                                ON UPDATE CASCADE
                                ON DELETE RESTRICT,
    currency       CHAR(3)      NOT NULL DEFAULT 'RUB'
                                CHECK (currency ~ '^[A-Z]{3}$'),
    opened_at      DATE         NOT NULL DEFAULT CURRENT_DATE,
    status         TEXT         NOT NULL DEFAULT 'active'
                                CHECK (status IN ('active','blocked','closed')),
    balance        NUMERIC(18,2) NOT NULL DEFAULT 0
                                CHECK (balance >= 0)
);

COMMENT ON TABLE accounts IS 'Счета клиентов';

CREATE INDEX idx_accounts_client_id ON accounts (client_id);
CREATE INDEX idx_accounts_status    ON accounts (status);
-- быстрый поиск активных счетов клиента
CREATE INDEX idx_accounts_client_active
    ON accounts (client_id)
    WHERE status = 'active';


-- ============================================================
-- 3. CARD
-- ============================================================
CREATE TABLE cards (
    card_id         TEXT        PRIMARY KEY
                                CHECK (card_id ~ '^card-[0-9]{5}$'),
    account_id      TEXT        NOT NULL
                                REFERENCES accounts (account_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
    client_id       TEXT        NOT NULL
                                REFERENCES clients (client_id)
                                ON UPDATE CASCADE
                                ON DELETE RESTRICT,
    card_type       TEXT        NOT NULL
                                CHECK (card_type IN ('debit','credit','virtual','prepaid')),
    payment_system  TEXT        NOT NULL
                                CHECK (payment_system IN ('VISA','MASTERCARD','MIR')),
    status          TEXT        NOT NULL DEFAULT 'active'
                                CHECK (status IN ('active','blocked','expired','closed')),
    issued_at       DATE        NOT NULL DEFAULT CURRENT_DATE,
    expires_at      DATE        NOT NULL,

    CONSTRAINT cards_expiry_after_issue CHECK (expires_at > issued_at)
);

COMMENT ON TABLE cards IS 'Карты (привязаны к счёту и клиенту для быстрых выборок)';

CREATE INDEX idx_cards_account_id      ON cards (account_id);
CREATE INDEX idx_cards_client_id       ON cards (client_id);
CREATE INDEX idx_cards_status          ON cards (status);
-- частичный индекс: активные карты
CREATE INDEX idx_cards_active
    ON cards (card_id, client_id)
    WHERE status = 'active';
-- связка «клиент + активная карта» — именно её использует итоговый запрос
CREATE INDEX idx_cards_active_by_client
    ON cards (client_id, card_id)
    WHERE status = 'active';


-- ============================================================
-- 4. DEVICE
-- ============================================================
CREATE TABLE devices (
    device_id       TEXT        PRIMARY KEY
                                CHECK (device_id ~ '^device-[0-9]{5}$'),
    client_id       TEXT        NOT NULL
                                REFERENCES clients (client_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
    device_type     TEXT        NOT NULL
                                CHECK (device_type IN ('mobile','tablet','desktop')),
    os              TEXT        NOT NULL
                                CHECK (os IN ('iOS','Android','Windows','macOS','Linux')),
    trusted         BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE devices IS 'Устройства клиентов';

CREATE INDEX idx_devices_client_id ON devices (client_id);
CREATE INDEX idx_devices_trusted   ON devices (client_id) WHERE trusted = TRUE;


-- ============================================================
-- 5. MERCHANT
-- ============================================================
CREATE TABLE merchants (
    merchant_id     TEXT        PRIMARY KEY
                                CHECK (merchant_id ~ '^merchant-[0-9]{4}$'),
    name            TEXT        NOT NULL,
    mcc             CHAR(4)     NOT NULL
                                CHECK (mcc ~ '^[0-9]{4}$'),
    country         CHAR(2)     NOT NULL,
    city            TEXT        NOT NULL,
    risk_level      TEXT        NOT NULL
                                CHECK (risk_level IN ('low','standard','high'))
);

COMMENT ON TABLE merchants IS 'Торговые точки';

CREATE INDEX idx_merchants_mcc      ON merchants (mcc);
CREATE INDEX idx_merchants_country  ON merchants (country);
CREATE INDEX idx_merchants_risk     ON merchants (risk_level);


