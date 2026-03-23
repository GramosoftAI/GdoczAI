CREATE TABLE IF NOT EXISTS document_logics (
    logic_type_id SERIAL PRIMARY KEY,
    logic_name VARCHAR(100) NOT NULL,
    logic_json JSONB,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);