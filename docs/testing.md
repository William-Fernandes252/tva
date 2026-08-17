# Testing Strategy

TVA embraces a multi-tiered testing strategy to ensure that both the pure business logic and the complex distributed interactions are thoroughly validated.

## 1. Unit Testing (Hspec)

The Haskell services use mtl-style typeclasses to abstract side-effects (`MonadStorage`, `MonadQueue`, `MonadDatabase`, `MonadTranscoder`). This allows us to write unit tests that execute the core business logic without needing any actual infrastructure.

Instead of running against a real PostgreSQL database or MinIO server, unit tests run against pure in-memory state monads (like `StateT`). This makes the unit tests extremely fast, deterministic, and isolated.

**Execution**:
Run `stack test` inside any of the individual service directories (e.g., `api-server`, `video-worker`).

## 2. End-to-End (E2E) Testing

While unit tests validate the logic, they cannot validate the network boundaries, database triggers, message serialization, and container orchestration. The E2E test suite validates the entire system flow identically to how a user interacts with it.

### E2E Test Workflow

The E2E suite (`e2e/test/E2ESpec.hs`) simulates a real client interacting with the deployed Docker ecosystem.

1. **Setup**: The `beforeAll` hook executes `docker compose -f docker-compose.test.yml up --build -d`. This creates a completely isolated ephemeral environment, overriding commands to run the compiled binaries. It waits for the API server's health check to pass.
2. **Upload URL**: The test client queries the `api-server` for an S3 presigned URL.
3. **MinIO Upload**: The test client uploads a tiny fixture video (`.mp4`) directly to the MinIO test container.
4. **Webhook Triggering**: In the testing environment, to avoid dependency on MinIO's asynchronous webhook mechanism, the test client manually POSTs the expected `s3:ObjectCreated:Put` payload to the `api-server` webhook endpoint.
5. **Processing Assertion**: The test polls the `api-server` status endpoint, asserting that the state eventually reaches `COMPLETED`.
6. **Storage Assertion**: The test queries the MinIO `processed-videos` bucket to assert that the `.m3u8` playlist and `.ts` segmented chunks were actually generated and stored.
7. **Teardown**: The `afterAll` hook aggressively tears down the Docker compose environment (`down -v`), ensuring no volume state leaks into subsequent test runs.

### Docker Parity

To ensure the test environment matches production, the tests leverage a **Multi-stage Dockerfile**.
- A **Builder stage** compiles all Haskell binaries using `haskell:9.4.8`, allowing GHC compilation cache sharing across all three services.
- A **Runner stage** (built on a slim Debian image) installs the runtime binaries (`ffmpeg`, `libpq-dev`) and executes the services.

**Execution**:
Navigate to the `e2e/` folder and execute:
```bash
stack test
```
*(Requires the Docker daemon to be running on the host machine)*
