CREATE TABLE IF NOT EXISTS silver.example (
    id SERIAL NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_silver_example_created_at ON silver.example(created_at);

CREATE INDEX IF NOT EXISTS idx_silver_example_updated_at ON silver.example(updated_at);
