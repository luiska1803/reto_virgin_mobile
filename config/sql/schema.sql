-- ========================================
--             SCHEMA PROPUESTO: 
-- ========================================

-- 1. Campañas
CREATE TYPE campaign_channel AS ENUM ('mobile_push', 'email', 'whatsapp', 'dashboard');

CREATE TABLE IF NOT EXISTS campaign (
    id BIGINT PRIMARY KEY,
    campaign_type VARCHAR(50),
    channel campaign_channel NOT NULL,
    topic VARCHAR(100),
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    total_count INT,
    warmup_mode BOOLEAN DEFAULT FALSE,
    subject_length INT,
    subject_with_personalization BOOLEAN DEFAULT FALSE,
    subject_with_deadline BOOLEAN DEFAULT FALSE,
    subject_with_emoji BOOLEAN DEFAULT FALSE,
    subject_with_bonuses BOOLEAN DEFAULT FALSE,
    subject_with_discount BOOLEAN DEFAULT FALSE,
    subject_with_saleout BOOLEAN DEFAULT FALSE
);

-- 2. Tabla de Clients
CREATE TYPE gender_channel AS ENUM ('female', 'male', 'other');

CREATE TABLE IF NOT EXISTS clients (
    client_id BIGINT PRIMARY KEY,
    first_purchase_date DATE,
    gender gender_channel NOT NULL,
    age INT,
    country VARCHAR(50),
    city VARCHAR(50),
    preferred_channel VARCHAR(20),
    loyalty_level VARCHAR(20),
    average_purchase_value NUMERIC(10,2),
    churn_risk NUMERIC(4,2)
);

-- 3. Tabla de holidays
CREATE TABLE IF NOT EXISTS holidays (
    id_holiday SERIAL PRIMARY KEY, 
    "date" DATE,
    year INT,
    month INT,
    day INT,
    weekday VARCHAR(20),
    holiday VARCHAR(100)
);

-- 4. Tabla de Messages 
CREATE TABLE IF NOT EXISTS messages (
    message_id VARCHAR(50) PRIMARY KEY,
    campaign_id BIGINT REFERENCES campaign(id) ON DELETE CASCADE,
    client_id BIGINT REFERENCES clients(client_id) ON DELETE CASCADE,
    message_type VARCHAR(50),
    channel VARCHAR(20),
    platform VARCHAR(50),
    email_provider VARCHAR(50),
    stream VARCHAR(50),
    "date" TIMESTAMP,
    sent_at TIMESTAMP,
    is_opened BOOLEAN DEFAULT FALSE,
    opened_first_time_at TIMESTAMP,
    opened_last_time_at TIMESTAMP,
    is_clicked BOOLEAN DEFAULT FALSE,
    is_unsubscribed BOOLEAN DEFAULT FALSE,
    is_hard_bounced BOOLEAN DEFAULT FALSE,
    is_soft_bounced BOOLEAN DEFAULT FALSE,
    is_complained BOOLEAN DEFAULT FALSE,
    is_blocked BOOLEAN DEFAULT FALSE,
    is_purchased BOOLEAN DEFAULT FALSE
);

-- ========================================
-- TABLA DE AGREGACION 
-- ========================================

-- 5. tabla de agregacion del performance de campaña
CREATE TABLE IF NOT EXISTS agg_campaign_performance (
    id_agg SERIAL PRIMARY KEY, 
    campaign_id INT REFERENCES campaign(id),
    total_sent INT DEFAULT 0,
    total_opened INT DEFAULT 0,
    total_clicked INT DEFAULT 0,  
    total_purchased INT DEFAULT 0,
    total_unsubscribed INT DEFAULT 0,
    total_bounced INT DEFAULT 0,
    open_rate NUMERIC(5,2) DEFAULT 0,
    click_rate NUMERIC(5,2) DEFAULT 0,
    conversion_rate NUMERIC(5,2) DEFAULT 0,
    unsubscribe_rate NUMERIC(5,2) DEFAULT 0,
    bounce_rate NUMERIC(5,2) DEFAULT 0
);

