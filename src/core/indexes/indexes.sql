-- Indexes for OCR document pipeline
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_document_types_user_id ON document_types(user_id);
CREATE INDEX IF NOT EXISTS idx_document_schemas_doc_type_id ON document_schemas(doc_type_id);
CREATE INDEX IF NOT EXISTS idx_document_logics_user_id ON document_logics(user_id);
CREATE INDEX IF NOT EXISTS idx_sftp_connector_user_id ON sftp_connector(user_id);
CREATE INDEX IF NOT EXISTS idx_smtp_connector_user_id ON smtp_connector(user_id);
CREATE INDEX IF NOT EXISTS idx_request_id ON processed_files(request_id);
CREATE INDEX IF NOT EXISTS idx_user_id ON processed_files(user_id);
CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files(file_path);
CREATE INDEX IF NOT EXISTS idx_olmocr_used ON processed_files(olmocr_used);
