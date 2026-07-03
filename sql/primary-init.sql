-- pg-source (PG17 PRIMARY) bootstrap
-- Streaming replication + Debezium failover slot için hazırlık.

-- Replication kullanıcısı (standby pg_basebackup ile bağlanır)
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';

-- CDC tablosu + seed
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE TABLE inventory.customers (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT
);
ALTER TABLE inventory.customers REPLICA IDENTITY FULL;
INSERT INTO inventory.customers (name, email) VALUES
    ('Ada Lovelace', 'ada@example.com'),
    ('Linus T.',     'linus@example.com');

-- Debezium publication (autocreate yerine önceden hazır)
CREATE PUBLICATION dbz_pub FOR TABLE inventory.customers;

-- Standby için fiziksel replication slot (basebackup -S ile kullanılır)
SELECT pg_create_physical_replication_slot('standby_phys_slot');

-- Debezium logical slot — failover=true => PG17 standby'a senkron edilir.
-- imza: pg_create_logical_replication_slot(name, plugin, temporary, twophase, failover)
SELECT pg_create_logical_replication_slot('dbz_slot', 'pgoutput', false, false, true);
