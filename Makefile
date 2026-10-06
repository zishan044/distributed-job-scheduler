.PHONY: up down logs build lint

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs --follow --tail=200

build:
	docker compose build

lint:
	@test -z "$$(gofmt -l $$(find cmd internal -type f -name '*.go'))"
	golangci-lint run ./...
