-- Kaynak DB seed: inventory.customers
CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.customers (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT
);

-- Debezium'un UPDATE/DELETE'lerde tam before-image üretmesi için
ALTER TABLE inventory.customers REPLICA IDENTITY FULL;

INSERT INTO inventory.customers (name, email) VALUES
    ('Ada Lovelace', 'ada@example.com'),
    ('Linus T.',     'linus@example.com');
