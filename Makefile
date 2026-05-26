.PHONY: dev test test-unit test-integration lint migrate-up migrate-down docker-build bootstrap

BINARY=pulse
MODULE=github.com/pulse
POSTGRES_URL?=postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable
MIGRATE=migrate -path migrations -database "pgx5://pulse:pulse@localhost:5433/pulse?sslmode=disable"

bootstrap:
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
	go mod download

migrate-up:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

migrate-drop:
	$(MIGRATE) drop -f

test-unit:
	go test ./internal/job/... -count=1

test-integration:
	go test ./internal/storage/... ./internal/queue/... -count=1 -race -tags integration

test: test-unit test-integration

lint:
	go vet ./...

dev-api:
	go run ./cmd/pulse --role api

dev-scheduler:
	go run ./cmd/pulse --role scheduler

dev-worker:
	go run ./cmd/pulse --role worker

docker-build:
	docker build -t pulse:dev .

up:
	docker compose up -d

down:
	docker compose down
