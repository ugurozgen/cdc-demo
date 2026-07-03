#!/bin/bash
# Primary init: uzaktan replication bağlantısına izin ver (lab: trust).
# Resmi postgres image'ı POSTGRES_HOST_AUTH_METHOD'u replication satırına uygulamaz.
set -e
{
  echo "host replication replicator all trust"
  echo "host replication all        all trust"
} >> "$PGDATA/pg_hba.conf"
echo "[primary] pg_hba.conf'a replication satırları eklendi"
