-- Schema fixture for GraphDB deployment matrix tests.
-- The runner creates the source/target databases; this file only creates objects inside them.

CREATE TABLE IF NOT EXISTS users (
    row_id INT NOT NULL AUTO_INCREMENT,
    user_id VARCHAR(32) NOT NULL,
    email VARCHAR(128) NOT NULL,
    display_name VARCHAR(128) DEFAULT NULL,
    active TINYINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (row_id),
    UNIQUE KEY uid_user_id (user_id),
    KEY idx_email (email),
    KEY idx_active (active)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS posts (
    row_id INT NOT NULL AUTO_INCREMENT,
    post_id VARCHAR(32) NOT NULL,
    author_user_id VARCHAR(32) NOT NULL,
    title VARCHAR(256) NOT NULL,
    body TEXT,
    published_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (row_id),
    UNIQUE KEY uid_post_id (post_id),
    KEY idx_author (author_user_id)
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW active_users AS
SELECT user_id, email, display_name
FROM users
WHERE active = 1;
