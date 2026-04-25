#!/bin/sh
set -e
DATA_DIR=/var/lib/postgresql/data
PRIMARY_HOST="${PRIMARY_HOST:-postgres}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
# Password for `postgres` superuser on the primary (used only to drop an inactive slot before re-bootstrap).
PRIMARY_SUPER_PASS="${PRIMARY_POSTGRES_PASSWORD:-${POSTGRES_PASSWORD:-postgres}}"

if [ -s "$DATA_DIR/PG_VERSION" ]; then
	# CMD is already "postgres"; do not duplicate (would become `postgres postgres` -> invalid).
	exec su-exec postgres docker-entrypoint.sh "$@"
fi

echo "[replica] waiting for primary postgres..."
until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U postgres -d postgres >/dev/null 2>&1; do
	sleep 1
done

# If this replica uses a named physical slot, drop it only when inactive (e.g. volume wiped and slot left on primary).
if [ -n "${REPLICATION_SLOT_NAME:-}" ]; then
	echo "[replica] clearing inactive replication slot ${REPLICATION_SLOT_NAME} on primary if present..."
	PGPASSWORD="$PRIMARY_SUPER_PASS" psql -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 -c \
		"SELECT pg_drop_replication_slot(s.slot_name) FROM pg_replication_slots s WHERE s.slot_name = '${REPLICATION_SLOT_NAME}' AND NOT s.active;"
fi

echo "[replica] wiping data dir and taking base backup from primary..."
rm -rf "$DATA_DIR"/*

export PGPASSWORD="${REPLICATOR_PASSWORD:-replicator_pass}"
if [ -n "${REPLICATION_SLOT_NAME:-}" ]; then
	echo "[replica] pg_basebackup with replication slot ${REPLICATION_SLOT_NAME}"
	pg_basebackup \
		-h "$PRIMARY_HOST" \
		-p "$PRIMARY_PORT" \
		-U replicator \
		-D "$DATA_DIR" \
		-Fp \
		-Xs \
		-P \
		-C \
		-S "$REPLICATION_SLOT_NAME" \
		-R
else
	pg_basebackup \
		-h "$PRIMARY_HOST" \
		-p "$PRIMARY_PORT" \
		-U replicator \
		-D "$DATA_DIR" \
		-Fp \
		-Xs \
		-P \
		-R
fi

echo "[replica] fixing permissions..."
chown -R postgres:postgres "$DATA_DIR"

echo "[replica] starting standby"
exec su-exec postgres docker-entrypoint.sh "$@"
