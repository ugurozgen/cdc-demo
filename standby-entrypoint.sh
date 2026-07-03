#!/bin/bash
# pg-source-ankara (PG17 STANDBY) entrypoint.
# PGDATA boşsa: pg-source-gebze'tan pg_basebackup ile fiziksel replica kur, sonra
# PG17 slot sync ayarlarını yaz. Doluysa: normal başlat (promote sonrası dahil).
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARY_HOST="${PRIMARY_HOST:-pg-source-gebze}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "[standby] PGDATA boş — pg-source-gebze bekleniyor..."
  until pg_isready -h "$PRIMARY_HOST" -p 5432 -U replicator -d sourcedb -q; do
    echo "[standby] primary hazır değil, 2sn..."; sleep 2
  done

  echo "[standby] pg_basebackup başlıyor (slot=standby_phys_slot)"
  rm -rf "${PGDATA:?}/"* 2>/dev/null || true
  PGPASSWORD=replicator gosu postgres pg_basebackup \
    -h "$PRIMARY_HOST" -p 5432 -U replicator \
    -D "$PGDATA" -Fp -Xs -P -R -S standby_phys_slot

  echo "[standby] slot sync + standby ayarları yazılıyor"
  # -R zaten standby.signal + primary_conninfo yazdı; PG17 slot sync için dbname şart.
  cat >> "$PGDATA/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=$PRIMARY_HOST port=5432 user=replicator password=replicator dbname=sourcedb application_name=ankara_standby'
primary_slot_name = 'standby_phys_slot'
hot_standby = on
hot_standby_feedback = on
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
sync_replication_slots = on
EOF
  chown -R postgres:postgres "$PGDATA"
  echo "[standby] hazır, postgres başlatılıyor"
else
  echo "[standby] PGDATA dolu — olduğu gibi başlatılıyor"
fi

exec docker-entrypoint.sh postgres
