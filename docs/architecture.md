# Architecture Overview

TVA is an asynchronous, event-driven video processing pipeline. It decouples the heavy lifting of video transcoding from the HTTP layer, ensuring scalability, resilience, and real-time frontend updates.

## System Components

The system is composed of three custom Haskell services and three infrastructure dependencies.

### Haskell Services

1. **API Server (`api-server`)**
   - **Role:** Handles client HTTP interactions, presigned URL generation, static file serving, and webhook ingestion.
   - **Responsibilities:**
     - Generates pre-signed MinIO upload URLs, allowing the browser to bypass the API server when uploading heavy video files.
     - Receives `s3:ObjectCreated` Webhooks from MinIO when a user finishes uploading a video.
     - Inserts the new job into the PostgreSQL database.
     - Publishes a `VideoUploadedEvent` to RabbitMQ.
     - Serves the frontend UI (`index.html`) and handles the HTTP router.
     - **Zombie Job Sweeper:** Runs a background thread that periodically identifies jobs stuck in the `PROCESSING` state for too long and reverts them to `PENDING` to ensure high availability.

2. **Video Worker (`video-worker`)**
   - **Role:** Consumes messages from RabbitMQ and executes the actual FFmpeg transcoding.
   - **Responsibilities:**
     - Listens to the `video_upload_queue` in RabbitMQ.
     - Downloads the raw video from MinIO using the AWS S3 APIs.
     - Spawns a shell `ffmpeg` process to transcode the video into HLS (`.m3u8` and `.ts` chunks).
     - Parses the `stderr` output of `ffmpeg` in real-time to calculate processing progress.
     - Updates the job status (`PROCESSING`, `COMPLETED`, `FAILED`) and the progress percentage in the PostgreSQL database.
     - Uploads the resulting HLS chunks back to the public `processed-videos` bucket in MinIO.
     - Emits Postgres `NOTIFY` events on status and progress changes.

3. **Notifier Worker (`notifier-worker`)**
   - **Role:** Bridges database state changes to the frontend via WebSockets.
   - **Responsibilities:**
     - Maintains persistent WebSocket connections with active client browsers.
     - Listens to PostgreSQL `LISTEN/NOTIFY` channels.
     - When the `video-worker` updates a job state or progress in the database, the trigger fires a Postgres `NOTIFY` event.
     - The notifier worker parses the JSON payload of the notification and multiplexes the message to the specific WebSocket client listening for that `videoId`.

### Infrastructure Dependencies

- **MinIO**: Used as an S3-compatible object storage layer. Hosts the `raw-videos` (private upload bucket) and `processed-videos` (public read bucket) buckets. MinIO is configured to emit a webhook back to the API server whenever a new file lands in `raw-videos`.
- **PostgreSQL**: The source of truth for the system's state. Stores video metadata, job statuses, and utilizes PostgreSQL triggers to emit `NOTIFY` events upon updates. Database schema migrations are managed via `dbmate`.
- **RabbitMQ**: The message broker that decouples the API server from the heavy transcoding workers. Employs a Dead-Letter Exchange (DLX) and Dead-Letter Queue (DLQ) topology to gracefully handle poison messages.

## Domain-Driven Design (DDD)

The services are built using mtl-style typeclasses (`MonadStorage`, `MonadQueue`, `MonadDatabase`) to abstract away side effects. This design allows the business logic to remain pure and highly testable, with implementations swapped out during testing.

## State Machine

A transcoding job transitions through the following states, tracked in the database:
1. `PENDING`: The video was uploaded, and the event was published to RabbitMQ.
2. `PROCESSING`: A video worker picked up the event, validated the file, and initiated `ffmpeg`. Progress is actively updated.
3. `COMPLETED`: Transcoding succeeded, and HLS chunks were successfully uploaded to the public bucket.
4. `FAILED`: Transcoding or validation failed, usually after exhausting RabbitMQ retries or encountering unrecoverable FFmpeg errors.
