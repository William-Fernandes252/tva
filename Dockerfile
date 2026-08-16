# Stage 1: Build all Haskell services
FROM haskell:9.4.8 AS builder

# Set the working directory
WORKDIR /app

# Install system dependencies needed for building (fixing Buster EOL apt sources)
RUN sed -i -e 's/deb.debian.org/archive.debian.org/g' \
           -e 's|security.debian.org|archive.debian.org/|g' \
           -e '/stretch-updates/d' \
           -e '/buster-updates/d' /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the stack configuration files
COPY stack.yaml stack.yaml.lock ./
COPY package.yaml ./
COPY api-server/package.yaml ./api-server/
COPY video-worker/package.yaml ./video-worker/
COPY notifier-worker/package.yaml ./notifier-worker/
COPY e2e/package.yaml ./e2e/

# Build dependencies only (caches dependencies)
RUN stack build --only-dependencies

# Copy the source code
COPY core-domain ./core-domain
COPY api-server ./api-server
COPY video-worker ./video-worker
COPY notifier-worker ./notifier-worker
COPY e2e ./e2e
COPY CHANGELOG.md LICENSE ./

# Build the executables and copy them to a known location
RUN stack build --copy-bins --local-bin-path /app/bin

# Stage 2: Create the minimal runner image
FROM debian:bookworm-slim AS runner

# Install runtime dependencies (FFmpeg, Postgres client libs, CA certs)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libpq5 \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m appuser
WORKDIR /app
RUN chown appuser:appuser /app

# Copy the compiled binaries from the builder stage
COPY --from=builder --chown=appuser:appuser /app/bin/tva-api-server /usr/local/bin/
COPY --from=builder --chown=appuser:appuser /app/bin/tva-video-worker /usr/local/bin/
COPY --from=builder --chown=appuser:appuser /app/bin/tva-notifier-worker /usr/local/bin/

USER appuser

# By default, don't run anything (entrypoint overridden in docker-compose)
CMD ["echo", "Please specify a command (e.g., tva-api-server)"]
