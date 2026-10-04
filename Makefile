export XOUTBOX_POSTGRES_DSN ?= postgres://postgres:postgres@localhost:55432/postgres?sslmode=disable
export XOUTBOX_MYSQL_DSN ?= root:root@tcp(localhost:53306)/outbox
export XOUTBOX_REDIS_ADDR ?= localhost:56379
export XOUTBOX_NATS_URL ?= nats://localhost:54222
export XOUTBOX_RABBITMQ_URL ?= amqp://guest:guest@localhost:55672/
export XOUTBOX_KAFKA_BROKERS ?= localhost:59092

.PHONY: test test-integration up down lint

test:
	go test -race ./...

up:
	docker compose up -d --wait

down:
	docker compose down -v

test-integration: up
	go test -race -count=1 ./...

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"
