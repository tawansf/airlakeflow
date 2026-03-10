CREATE TABLE IF NOT EXISTS bronze.task (
    id SERIAL NOT NULL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_task_created_at ON bronze.task(created_at);

CREATE INDEX IF NOT EXISTS idx_bronze_task_updated_at ON bronze.task(updated_at);
