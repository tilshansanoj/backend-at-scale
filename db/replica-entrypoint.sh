#!/bin/sh
set -e
DATA_DIR=/var/lib/postgresql/data

if [ -s "$DATA_DIR/PG_VERSION" ]; then
	# CMD is already "postgres"; do not duplicate (would become `postgres postgres` -> invalid).
	exec su-exec postgres docker-entrypoint.sh "$@"
fi

echo "[replica] waiting for primary postgres..."
until pg_isready -h postgres -p 5432 -U postgres -d postgres >/dev/null 2>&1; do
	sleep 1
done

echo "[replica] wiping data dir and taking base backup from primary..."
rm -rf "$DATA_DIR"/*

export PGPASSWORD="${REPLICATOR_PASSWORD:-replicator_pass}"
pg_basebackup \
	-h postgres \
	-p 5432 \
	-U replicator \
	-D "$DATA_DIR" \
	-Fp \
	-Xs \
	-P \
	-R

echo "[replica] fixing permissions..."
chown -R postgres:postgres "$DATA_DIR"

echo "[replica] starting standby"
exec su-exec postgres docker-entrypoint.sh "$@"
