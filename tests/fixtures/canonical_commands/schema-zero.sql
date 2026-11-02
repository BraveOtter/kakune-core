CREATE TABLE workflows (
  id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, source TEXT NOT NULL,
  status TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE node_runs (
  id INTEGER PRIMARY KEY, execution_id TEXT NOT NULL, node_id TEXT NOT NULL,
  status TEXT NOT NULL, message TEXT, created_at TEXT NOT NULL
);
CREATE TABLE auth_tokens (
  id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL, revoked_at TEXT
);
INSERT INTO workflows VALUES ('legacy-fixture', 'Legacy fixture', 'fixture source', 'disabled', '2026-01-01T00:00:00Z');
