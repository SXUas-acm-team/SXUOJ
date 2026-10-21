-- Run against the existing HOJ database before starting v1.1.0.
-- Adds four tables only; existing business tables and data are unchanged.
CREATE TABLE IF NOT EXISTS contest_identity_config (
  cid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  enabled TINYINT(1) NOT NULL DEFAULT 0,
  replace_realname TINYINT(1) NOT NULL DEFAULT 0,
  star_prefixes VARCHAR(1536) NOT NULL DEFAULT '',
  api_url VARCHAR(1024) NOT NULL,
  api_key TEXT NULL COMMENT 'AES-GCM encrypted; never return to browser',
  gmt_create DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_identity_config_contest FOREIGN KEY (cid) REFERENCES contest(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS contest_identity_invitation (
  cid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  password_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  CONSTRAINT fk_identity_invitation_contest FOREIGN KEY (cid) REFERENCES contest(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS contest_identity_exemption (
  cid BIGINT UNSIGNED NOT NULL,
  uid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  invitation_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (cid, uid),
  CONSTRAINT fk_identity_exemption_contest FOREIGN KEY (cid) REFERENCES contest(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS contest_identity_binding (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cid BIGINT UNSIGNED NOT NULL,
  uid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  student_number VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  verified_name VARCHAR(128) NOT NULL,
  verified_at DATETIME NOT NULL,
  UNIQUE KEY uk_contest_identity_user (cid, uid),
  UNIQUE KEY uk_contest_identity_student (cid, student_number),
  CONSTRAINT fk_identity_binding_contest FOREIGN KEY (cid) REFERENCES contest(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
