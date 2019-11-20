BINARY_NAME=candles


all: lint build
build:
	go build -race -v -o $(BINARY_NAME)
lint:
	golangci-lint run
