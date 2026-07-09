CREATE TABLE IF NOT EXISTS smtp_connector (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    email_id VARCHAR(255) NOT NULL,
    app_password VARCHAR(255) NOT NULL,
    approved_senders TEXT,
    imap_server VARCHAR(255),
    imap_port INTEGER,
    interval_minute INTEGER DEFAULT 10,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);