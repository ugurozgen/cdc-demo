#!/bin/bash
# reprovision-gebze: gebze'yi ANKARA'nın standby'ı olarak yeniden kurar (production-doğru failback).
# Geride kalan eski primary (gebze), ileri olan güncel primary'den (ankara) pg_basebackup ile
# standby'a döner -> veri kaybı yok. (Patroni'nin pg_rewind ile otomatik yaptığının basebackup hali.)
# Çağrı: docker compose run --rm --no-deps --entrypoint bash pg-source-gebze /reprovision-gebze.sh
# postgres BAŞLAMADAN önce PGDATA'yı doldurur; sonra 'docker compose up -d pg-source-gebze' standby'ı başlatır.
set -e
PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARY_HOST="pg-source-ankara"   # güncel primary (failover sonrası)
SLOT="gebze_phys_slot"            # ankara'da bu slot önceden oluşturulur (Makefile)

echo "[reprov-gebze] ankara (yeni primary) bekleniyor..."
until pg_isready -h "$PRIMARY_HOST" -p 5432 -U replicator -d sourcedb -q; do
  echo "[reprov-gebze] ankara hazır değil, 2sn..."; sleep 2
done

echo "[reprov-gebze] eski gebze PGDATA temizleniyor (stale primary verisi atılıyor)"
rm -rf "${PGDATA:?}/"* 2>/dev/null || true

echo "[reprov-gebze] ankara'dan pg_basebackup (slot=$SLOT)"
PGPASSWORD=replicator gosu postgres pg_basebackup \
  -h "$PRIMARY_HOST" -p 5432 -U replicator \
  -D "$PGDATA" -Fp -Xs -P -R -S "$SLOT"

echo "[reprov-gebze] standby + slot sync ayarları yazılıyor"
# -R primary_conninfo yazdı; slot sync için dbname şart -> tam conninfo'yu son satır olarak ekliyoruz (last-wins).
cat >> "$PGDATA/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=$PRIMARY_HOST port=5432 user=replicator password=replicator dbname=sourcedb application_name=gebze_standby'
primary_slot_name = '$SLOT'
hot_standby = on
sync_replication_slots = on
EOF
chown -R postgres:postgres "$PGDATA"
echo "[reprov-gebze] tamam — gebze artık ankara'nın standby'ı olarak başlatılabilir"
