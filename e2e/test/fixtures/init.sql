CREATE TABLE video_jobs (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    source_url TEXT NOT NULL,
    assigned_worker_id UUID,
    progress_percent INT,
    output_chunks TEXT[],
    error_message TEXT
);
