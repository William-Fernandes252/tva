# TVA - Typed Video API

An event-driven, scalable video transcoding pipeline built with Haskell.

TVA automatically processes uploaded videos (e.g., MP4, MKV) by transcoding them into **HLS (HTTP Live Streaming)** streams with multi-bitrate segmented chunks, enabling seamless web playback. 

The architecture is built on **Haskell**, **PostgreSQL**, **RabbitMQ**, and **MinIO** (S3-compatible storage), utilizing a robust event-sourcing and worker-based design.

## Features

- **Event-Driven Architecture**: Video uploads to MinIO trigger S3 Webhooks, queuing transcoding jobs in RabbitMQ.
- **Robust Workers**: Video processing is handled by dedicated background workers, with automated tracking of `PENDING`, `PROCESSING`, `COMPLETED`, and `FAILED` states.
- **WebSocket Progress Updates**: Real-time transcoding progress is streamed to the frontend via Postgres NOTIFY and WebSockets.
- **HLS Web Playback**: Successfully transcoded videos are served instantly to the browser using `hls.js`.

---

## 🚀 Getting Started

The easiest way to see the system in action is via Docker Compose. The entire environment (databases, queues, object storage, API, and workers) is containerized for a zero-configuration setup.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)

### 1. Spin up the environment

Run the following command at the root of the repository. It will build the Haskell services and initialize the MinIO buckets, PostgreSQL tables, and RabbitMQ queues.

```bash
docker compose up -d --build
```

*(Note: The initial Haskell compilation inside the Docker builder stage may take a few minutes.)*

### 2. Access the UI

Once the services are up and healthy, open your browser and navigate to:

👉 **[http://localhost:8080/](http://localhost:8080/)**

### 3. Test the Pipeline

1. **Upload a video**: Click "Choose File" and select an `.mp4` or `.mkv` video from your computer, then click **Upload & Process**.
2. **Watch the progress**: The video will be uploaded directly to MinIO, and you will see real-time WebSocket progress updates as FFmpeg chunks the video.
3. **Playback**: Once it reaches 100%, the browser will automatically initialize the HLS player and begin streaming the transcoded video directly from the MinIO `processed-videos` bucket.

---

## 🛠 Local Development

If you wish to run the Haskell services locally outside of Docker for development purposes, you will need the [Haskell Stack](https://docs.haskellstack.org/en/stable/README/) installed.

You can still use Docker to spin up the infrastructure dependencies:
```bash
docker compose up -d postgres rabbitmq minio dbmate
```

Then, you can build and run the services natively:
```bash
# Start the API server
stack run tva-api-server

# Start the Transcoding Worker
stack run tva-video-worker

# Start the WebSocket Notifier
stack run tva-notifier-worker
```

---

## 📚 Architecture & Documentation

For a deeper dive into the architectural decisions, domain models, and testing strategies used in this project, refer to the documentation:

- **[Architecture Overview](./docs/architecture.md)**: Details the event-driven decoupling, state machine transitions, and background workers.
- **[Testing Strategy](./docs/testing.md)**: Explains the simulated MinIO webhook approach and Hspec containerized test suite.
