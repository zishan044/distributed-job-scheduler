# Distributed Job Scheduler

Go-based distributed job scheduler with persisted jobs and an API. Job execution is planned for a later phase.

## quickstart

Requirements: Docker with the Compose plugin. Copy `.env.example` to `.env` if you want to override the local Postgres credentials, then run:

```sh
make up
```

Compose starts Postgres, Redis, Kafka in KRaft mode, initializes the Kafka topics, runs the Goose migrator, and starts one API, watcher, and executor. Only the API port is published to the host:

```sh
curl http://localhost:8080/healthz
docker compose ps
```

The API supports job creation and lookup, run listing and lookup, run-now, and cancellation. Send `Authorization: Bearer scheduler-local` with `/v1` requests, or set `API_KEY` in `.env` to use another key. Job execution is not available yet; watcher and executor currently expose health endpoints only.

The migrator applies the embedded Goose schema migration before the API starts. Redis is used for create-request idempotency, with Postgres preserving correctness if Redis is unavailable.

Stop the stack with `make down`. Named volumes are kept, so database and broker state survives. To deliberately remove local data, run `docker compose down --volumes`.

## Development

The module uses Go 1.26.3. Run `make build` to build the service images, `make test` with Docker available for Postgres/Redis integration tests, and `make lint` with golangci-lint v2.14.0 installed to check formatting and lint.
