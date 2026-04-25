-- Physical replication role (used by postgres-replica pg_basebackup).
-- Runs before init.sql (lexicographic order: 02-* before init.sql).

DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
		CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD 'replicator_pass';
	END IF;
END
$$;
