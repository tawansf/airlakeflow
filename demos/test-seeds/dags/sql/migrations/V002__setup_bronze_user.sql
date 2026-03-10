CREATE TABLE IF NOT EXISTS bronze.user (
    id SERIAL NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_user_created_at ON bronze.user(created_at);

CREATE INDEX IF NOT EXISTS idx_bronze_user_updated_at ON bronze.user(updated_at);
