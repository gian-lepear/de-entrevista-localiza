-- CREATE TABLE tmp_fraud_credit (
--     timestamp DATETIME,
--     sending_address VARCHAR(255),
--     receiving_address VARCHAR(255),
--     amount NUMERIC(18, 2),
--     transaction_type VARCHAR(255),
--     location_region VARCHAR(255),
--     ip_prefix NUMERIC(18, 2),
--     login_frequency INT,
--     session_duration INT,
--     purchase_pattern VARCHAR(255),
--     age_group VARCHAR(255),
--     risk_score NUMERIC(18, 2),
--     anomaly VARCHAR(255)
-- );

CREATE TABLE fraud_credit (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    sending_address VARCHAR(255) NOT NULL,
    receiving_address VARCHAR(255) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    transaction_type VARCHAR(255) NOT NULL,
    location_region VARCHAR(255) NOT NULL,
    ip_prefix NUMERIC(18, 2) NOT NULL,
    login_frequency INT NOT NULL,
    session_duration INT NOT NULL,
    purchase_pattern VARCHAR(255) NOT NULL,
    age_group VARCHAR(255) NOT NULL,
    risk_score NUMERIC(18, 2) NOT NULL,
    anomaly VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);


CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON fraud_credit
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();