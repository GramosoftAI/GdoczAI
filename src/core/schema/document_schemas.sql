CREATE TABLE IF NOT EXISTS document_schemas (
    schema_id SERIAL PRIMARY KEY,
    doc_type_id INTEGER REFERENCES document_types(doc_type_id) ON DELETE CASCADE,
    schema_name VARCHAR(100) NOT NULL,
    field_type VARCHAR(50),
    is_required BOOLEAN DEFAULT FALSE,
    default_value TEXT,
    validation_rules TEXT,
    prompt_field TEXT,
    logic_type_id INTEGER,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);