-- Stratum 元数据库结构（SQLite / WAL）
-- 设计要点：
--   1. object_versions 只存元数据，正文按内容寻址落在 CAS 文件系统；
--   2. chunk_refs 描述「版本 -> 分片序列」，chunks 保存引用计数做 O(1) 回收；
--   3. 所有时间字段为可字典序排序的 UTC 字符串，避免跨驱动时区差异；
--   4. 部分唯一索引保证「每个 key 至多一个 current 版本」，把不变量下沉到存储层。

CREATE TABLE IF NOT EXISTS schema_migrations (
  version    INTEGER PRIMARY KEY,
  name       TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
  name                 TEXT PRIMARY KEY,
  created_at           TEXT NOT NULL,
  versioning           INTEGER NOT NULL DEFAULT 0,
  quota_bytes          INTEGER NOT NULL DEFAULT 0,   -- 0 = 不限
  max_object_bytes     INTEGER NOT NULL DEFAULT 0,   -- 0 = 用全局上限
  used_bytes           INTEGER NOT NULL DEFAULT 0,
  object_count         INTEGER NOT NULL DEFAULT 0,
  default_content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  lifecycle_json       TEXT NOT NULL DEFAULT '{}',
  webhook_url          TEXT,
  labels_json          TEXT NOT NULL DEFAULT '{}',
  created_by           TEXT
);

CREATE TABLE IF NOT EXISTS object_versions (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  bucket        TEXT NOT NULL,
  key           TEXT NOT NULL,
  version_id    TEXT NOT NULL,
  is_delete_marker INTEGER NOT NULL DEFAULT 0,
  is_current    INTEGER NOT NULL DEFAULT 1,
  size          INTEGER NOT NULL DEFAULT 0,
  chunk_count   INTEGER NOT NULL DEFAULT 0,
  etag          TEXT NOT NULL DEFAULT '',
  content_type  TEXT,
  storage_class TEXT NOT NULL DEFAULT 'hot',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at    TEXT NOT NULL,
  created_by    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_versions_pk
  ON object_versions (bucket, key, version_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_versions_current
  ON object_versions (bucket, key) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS ix_versions_scan
  ON object_versions (bucket, key, created_at);
CREATE INDEX IF NOT EXISTS ix_versions_age
  ON object_versions (bucket, created_at);

CREATE TABLE IF NOT EXISTS chunks (
  hash        TEXT PRIMARY KEY,
  size        INTEGER NOT NULL,
  refcount    INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL,
  last_ref_at TEXT NOT NULL,
  corrupt     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_chunks_gc ON chunks (refcount, last_ref_at);

CREATE TABLE IF NOT EXISTS chunk_refs (
  bucket     TEXT NOT NULL,
  key        TEXT NOT NULL,
  version_id TEXT NOT NULL,
  seq        INTEGER NOT NULL,
  hash       TEXT NOT NULL,
  size       INTEGER NOT NULL,
  PRIMARY KEY (bucket, key, version_id, seq)
);

CREATE INDEX IF NOT EXISTS ix_chunk_refs_hash ON chunk_refs (hash);

CREATE TABLE IF NOT EXISTS multipart_uploads (
  upload_id    TEXT PRIMARY KEY,
  bucket       TEXT NOT NULL,
  key          TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  content_type TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  state        TEXT NOT NULL DEFAULT 'OPEN',
  created_by   TEXT,
  parts_bytes  INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_uploads_state ON multipart_uploads (state, created_at);

CREATE TABLE IF NOT EXISTS multipart_parts (
  upload_id  TEXT NOT NULL,
  part_no    INTEGER NOT NULL,
  hash       TEXT NOT NULL,
  size       INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (upload_id, part_no)
);

CREATE TABLE IF NOT EXISTS audit_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  ts          TEXT NOT NULL,
  actor       TEXT NOT NULL,
  action      TEXT NOT NULL,
  bucket      TEXT,
  key         TEXT,
  version_id  TEXT,
  status      INTEGER NOT NULL,
  request_id  TEXT,
  detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_audit_ts ON audit_log (ts);
CREATE INDEX IF NOT EXISTS ix_audit_bucket ON audit_log (bucket, ts);

CREATE TABLE IF NOT EXISTS outbox_events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at   TEXT NOT NULL,
  bucket       TEXT NOT NULL,
  event_type   TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  target_url   TEXT,
  state        TEXT NOT NULL DEFAULT 'PENDING',
  attempts     INTEGER NOT NULL DEFAULT 0,
  next_at      TEXT NOT NULL,
  last_error   TEXT,
  delivered_at TEXT
);

CREATE INDEX IF NOT EXISTS ix_outbox_due ON outbox_events (state, next_at);

CREATE TABLE IF NOT EXISTS access_keys (
  access_key  TEXT PRIMARY KEY,
  secret_hash TEXT NOT NULL,
  label       TEXT NOT NULL,
  grants_json TEXT NOT NULL DEFAULT '[]',
  is_admin    INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL,
  revoked_at  TEXT
);

CREATE TABLE IF NOT EXISTS maintenance_runs (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  job         TEXT NOT NULL,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  status      TEXT NOT NULL,
  result_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS scrub_reports (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id        INTEGER,
  started_at    TEXT NOT NULL,
  finished_at   TEXT,
  chunks_checked INTEGER NOT NULL DEFAULT 0,
  chunks_bad    INTEGER NOT NULL DEFAULT 0,
  bytes_read    INTEGER NOT NULL DEFAULT 0,
  detail_json   TEXT NOT NULL DEFAULT '[]'
);
