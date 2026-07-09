-- Trigger function to update updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger function to update updated_on column
CREATE OR REPLACE FUNCTION update_updated_on_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_on = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Example trigger for documents table
DROP TRIGGER IF EXISTS documents_updated_at_trigger ON documents;
CREATE TRIGGER documents_updated_at_trigger
BEFORE UPDATE ON documents
FOR EACH ROW EXECUTE FUNCTION update_updated_on_column();

-- Example trigger for processed_files table
DROP TRIGGER IF EXISTS processed_files_updated_on_trigger ON processed_files;
CREATE TRIGGER processed_files_updated_on_trigger
BEFORE UPDATE ON processed_files
FOR EACH ROW EXECUTE FUNCTION update_updated_on_column();

-- Add similar triggers for other tables as needed
