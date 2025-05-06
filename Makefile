VERSION=$(shell git describe --tags --abbrev=0 | sed 's/.*+feat\.//')

build:
	mkdir -p bin
	go build \
		-ldflags="-s -w -X github.com/ringsaturn/gawamango/internal/proxy.Version=${VERSION}" \
		-o bin/gawamango cmd/gawamango/main.go

run:
	go run cmd/gawamango/main.go > run.log 2>&1

run-with-stdout:
	OTEL_EXPORTER_TYPE=stdout go run cmd/gawamango/main.go > run.log 2>&1

run-with-otlp:
	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 go run cmd/gawamango/main.go > run.log 2>&1

clean:
	rm -rf bin

test:
	go test -v ./...

generate:
	go tool stringer -type OpCode internal/protocol/header.go
