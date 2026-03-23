CREATE TABLE IF NOT EXISTS sftp_connector (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    host_name VARCHAR(255) NOT NULL,
    port INTEGER DEFAULT 22,
    username VARCHAR(100) NOT NULL,
    password VARCHAR(255),
    private_key_path TEXT,
    monitor_folders TEXT,
    moved_folder TEXT,
    failed_folder TEXT,
    interval_minute INTEGER DEFAULT 10,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);