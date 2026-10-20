-- Only for deployments whose hoj.contest.id is BIGINT UNSIGNED (the supplied backup).
-- Check first: SHOW COLUMNS FROM hoj.contest LIKE 'id';
-- Existing policy/resource rows are retained. No existing business table is altered.
USE hoj;

CREATE TABLE IF NOT EXISTS contest_access_policy (
  cid BIGINT UNSIGNED NOT NULL,
  release_at DATETIME NULL DEFAULT NULL,
  gmt_create DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (cid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS contest_protected_resource (
  cid BIGINT UNSIGNED NOT NULL,
  resource_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  resource_path TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  PRIMARY KEY (cid, resource_hash),
  KEY idx_contest_resource_hash (resource_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
