CREATE SCHEMA metadata;

CREATE TABLE metadata.etl_logs(
    log_id        UUID PRIMARY KEY,
    job_name      VARCHAR(128) NOT NULL,
    run_start     TIMESTAMP NOT NULL,
    run_end       TIMESTAMP,
    status        VARCHAR(16),
    rows_loaded   INTEGER,
    rows_updated  INTEGER,
    rows_deleted  INTEGER,
    error_message TEXT
); 
