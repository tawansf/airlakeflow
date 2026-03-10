CREATE TABLE IF NOT EXISTS silver.task (
    id SERIAL NOT NULL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_silver_task_created_at ON silver.task(created_at);

CREATE INDEX IF NOT EXISTS idx_silver_task_updated_at ON silver.task(updated_at);
