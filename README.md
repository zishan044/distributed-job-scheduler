# Distributed Job Scheduler

Go-based distributed job scheduler. M0 provides the service skeleton and local dependencies; job scheduling and execution arrive in later milestones.

## M0 quickstart

Requirements: Docker with the Compose plugin. Copy `.env.example` to `.env` if you want to override the local Postgres credentials, then run:

```sh
make up
```

Compose starts Postgres, Redis, Kafka in KRaft mode, initializes the Kafka topics, runs the Goose migrator, and starts one API, watcher, and executor. Only the API port is published to the host:

```sh
curl http://localhost:8080/healthz
docker compose ps
```

The API, watcher, and executor currently expose health endpoints only. Their job-processing behavior is added in later milestones.

M0 has no schema migrations yet. The migrator treats Goose's `ErrNoMigrations` as success after confirming the database is reachable. M1 adds the first schema migration.

Stop the stack with `make down`. Named volumes are kept, so database and broker state survives. To deliberately remove local data, run `docker compose down --volumes`.

## Development

The module uses Go 1.26.3. Run `make build` to build the service images and `make lint` with golangci-lint v2.14.0 installed to check formatting and lint.
