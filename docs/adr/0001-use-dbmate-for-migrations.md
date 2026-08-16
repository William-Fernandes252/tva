# Use dbmate for Database Migrations

We decided to use `dbmate` as an external, language-agnostic tool for our database schema migrations rather than a Haskell-native library (like `hasql-migration`). This allows our Haskell microservices (`api-server` and `video-worker`) to remain completely decoupled from the database lifecycle.

By running `dbmate` as an ephemeral init container within our Docker setups (`docker-compose.yml` and `docker-compose.test.yml`), we ensure the database is fully migrated before the services start up. This eliminates race conditions during boot and prevents the services from needing embedded schema definition tools. Additionally, `dbmate` automatically dumps a `schema.sql` snapshot that provides a clear representation of the entire database state without requiring developers to piece together incremental migrations.
