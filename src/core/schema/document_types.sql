CREATE TABLE IF NOT EXISTS document_types (
    doc_type_id SERIAL PRIMARY KEY,
    document_type VARCHAR(100) NOT NULL,
    conditional_keys TEXT,
    langchain_keys TEXT,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(document_type, user_id)
);