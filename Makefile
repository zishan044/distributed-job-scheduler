.PHONY: up down logs build test lint smoke bench chaos

BENCH_PROFILE ?= durable
BENCH_SCENARIO ?= setup
CHAOS_COUNT ?= 1

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs --follow --tail=200

build:
	docker compose build

smoke:
	docker compose run --build --rm --no-deps smoke

bench:
	bash scripts/bench.sh "$(BENCH_PROFILE)" "$(BENCH_SCENARIO)"

chaos:
	bash scripts/chaos.sh "$(CHAOS_COUNT)"

test:
	go test -count=1 ./...
	go test -count=1 -tags=integration ./test/integration

lint:
	@test -z "$$(gofmt -l $$(find cmd internal pkg examples test -type f -name '*.go'))"
	golangci-lint run ./...
