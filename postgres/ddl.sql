-- ============================================================
-- Мастер-данные: clients, accounts, cards, devices, merchants
-- PostgreSQL 14+
-- ============================================================

CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core, public;
SET TIME ZONE 'UTC';


-- ============================================================
-- 1. CLIENT
-- ============================================================
CREATE TABLE IF NOT EXISTS core.clients (
    client_id    TEXT         PRIMARY KEY
                              CHECK (client_id ~ '^client-[0-9]{5}$'),
    created_at   DATE         NOT NULL DEFAULT CURRENT_DATE,
    country      CHAR(2)      NOT NULL
                              CHECK (country ~ '^[A-Z]{2}$'),
    city         TEXT         NOT NULL
                              CHECK (length(trim(city)) > 0),
    risk_level   TEXT         NOT NULL
                              CHECK (risk_level IN ('low','standard','high')),
    kyc_status   TEXT         NOT NULL
                              CHECK (kyc_status IN ('pending','verified','rejected'))
);

COMMENT ON TABLE  core.clients IS 'Клиенты (мастер-данные)';
COMMENT ON COLUMN core.clients.kyc_status IS 'pending / verified / rejected';

CREATE INDEX IF NOT EXISTS idx_clients_kyc_status    ON core.clients (kyc_status);
CREATE INDEX IF NOT EXISTS idx_clients_risk_level    ON core.clients (risk_level);
CREATE INDEX IF NOT EXISTS idx_clients_country_city  ON core.clients (country, city);
-- частичный индекс: клиенты с неподтверждённым KYC — частый фильтр для antifraud
CREATE INDEX IF NOT EXISTS idx_clients_kyc_unverified
    ON core.clients (client_id)
    WHERE kyc_status <> 'verified';


-- ============================================================
-- 2. ACCOUNT
-- ============================================================
CREATE TABLE IF NOT EXISTS core.accounts (
    account_id   TEXT          PRIMARY KEY
                               CHECK (account_id ~ '^account-[0-9]{6}$'),
    client_id    TEXT          NOT NULL
                               REFERENCES core.clients (client_id)
                               ON UPDATE CASCADE
                               ON DELETE RESTRICT,
    currency     CHAR(3)       NOT NULL DEFAULT 'RUB'
                               CHECK (currency ~ '^[A-Z]{3}$'),
    opened_at    DATE          NOT NULL DEFAULT CURRENT_DATE,
    status       TEXT          NOT NULL DEFAULT 'active'
                               CHECK (status IN ('active','blocked','closed')),
    balance      NUMERIC(18,2) NOT NULL DEFAULT 0
                               CHECK (balance >= 0)
);

COMMENT ON TABLE core.accounts IS 'Счета клиентов';

CREATE INDEX IF NOT EXISTS idx_accounts_client_id ON core.accounts (client_id);
CREATE INDEX IF NOT EXISTS idx_accounts_status    ON core.accounts (status);
CREATE INDEX IF NOT EXISTS idx_accounts_client_active
    ON core.accounts (client_id)
    WHERE status = 'active';


-- ============================================================
-- 3. CARD
-- ============================================================
CREATE TABLE IF NOT EXISTS core.cards (
    card_id         TEXT        PRIMARY KEY
                                CHECK (card_id ~ '^card-[0-9]{5}$'),
    account_id      TEXT        NOT NULL
                                REFERENCES core.accounts (account_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
    client_id       TEXT        NOT NULL
                                REFERENCES core.clients (client_id)
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

COMMENT ON TABLE core.cards IS 'Карты (привязаны к счёту и клиенту для быстрых выборок)';

CREATE INDEX IF NOT EXISTS idx_cards_account_id  ON core.cards (account_id);
CREATE INDEX IF NOT EXISTS idx_cards_client_id   ON core.cards (client_id);
CREATE INDEX IF NOT EXISTS idx_cards_status      ON core.cards (status);
-- частичный индекс: активные карты
CREATE INDEX IF NOT EXISTS idx_cards_active
    ON core.cards (card_id, client_id)
    WHERE status = 'active';
-- связка «клиент + активная карта» — под запрос «активные карты клиента»
CREATE INDEX IF NOT EXISTS idx_cards_active_by_client
    ON core.cards (client_id, card_id)
    WHERE status = 'active';


-- ============================================================
-- 4. DEVICE
-- ============================================================
CREATE TABLE IF NOT EXISTS core.devices (
    device_id      TEXT        PRIMARY KEY
                               CHECK (device_id ~ '^device-[0-9]{5}$'),
    client_id      TEXT        NOT NULL
                               REFERENCES core.clients (client_id)
                               ON UPDATE CASCADE
                               ON DELETE CASCADE,
    device_type    TEXT        NOT NULL
                               CHECK (device_type IN ('mobile','tablet','desktop')),
    os             TEXT        NOT NULL
                               CHECK (os IN ('iOS','Android','Windows','macOS','Linux')),
    trusted        BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE core.devices IS 'Устройства клиентов';

CREATE INDEX IF NOT EXISTS idx_devices_client_id ON core.devices (client_id);
CREATE INDEX IF NOT EXISTS idx_devices_trusted
    ON core.devices (client_id)
    WHERE trusted = TRUE;


-- ============================================================
-- 5. MERCHANT
-- ============================================================
CREATE TABLE IF NOT EXISTS core.merchants (
    merchant_id  TEXT      PRIMARY KEY
                           CHECK (merchant_id ~ '^merchant-[0-9]{4,5}$'),
    name         TEXT      NOT NULL
                           CHECK (length(trim(name)) > 0),
    mcc          CHAR(4)   NOT NULL
                           CHECK (mcc ~ '^[0-9]{4}$'),
    country      CHAR(2)   NOT NULL
                           CHECK (country ~ '^[A-Z]{2}$'),
    city         TEXT      NOT NULL,
    risk_level   TEXT      NOT NULL
                           CHECK (risk_level IN ('low','standard','high'))
);

COMMENT ON TABLE core.merchants IS 'Торговые точки';

CREATE INDEX IF NOT EXISTS idx_merchants_mcc     ON core.merchants (mcc);
CREATE INDEX IF NOT EXISTS idx_merchants_country ON core.merchants (country);
CREATE INDEX IF NOT EXISTS idx_merchants_risk    ON core.merchants (risk_level);

