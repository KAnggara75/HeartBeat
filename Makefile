APP_NAME := heartbeat
BIN_DIR := bin

.PHONY: all build run test clean docker-build docker-up docker-down

all: test build

build:
	@echo "==> Building binary..."
	@mkdir -p $(BIN_DIR)
	go build -ldflags="-w -s" -o $(BIN_DIR)/$(APP_NAME) ./cmd/heartbeat

run:
	@echo "==> Running HeartBeat service..."
	go run ./cmd/heartbeat -config config.yaml

run-once:
	@echo "==> Running HeartBeat one-shot..."
	go run ./cmd/heartbeat -config config.yaml -once

test:
	@echo "==> Running unit tests..."
	go test -v -race ./...

clean:
	@echo "==> Cleaning build artifacts..."
	rm -rf $(BIN_DIR)

docker-build:
	@echo "==> Building Docker image..."
	docker build -t $(APP_NAME):latest .

docker-up:
	@echo "==> Starting container with docker-compose..."
	docker compose up -d

docker-down:
	@echo "==> Stopping container..."
	docker compose down
