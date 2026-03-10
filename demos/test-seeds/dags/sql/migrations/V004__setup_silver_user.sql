CREATE TABLE IF NOT EXISTS silver.user (
    id SERIAL NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_silver_user_created_at ON silver.user(created_at);

CREATE INDEX IF NOT EXISTS idx_silver_user_updated_at ON silver.user(updated_at);
