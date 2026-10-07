.PHONY: up down logs build test lint

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs --follow --tail=200

build:
	docker compose build

test:
	go test -count=1 -tags=integration ./test/integration

lint:
	@test -z "$$(gofmt -l $$(find cmd internal test -type f -name '*.go'))"
	golangci-lint run ./...
