# MangoPi - MongoDB Proxy

MangoPi is a minimal MongoDB proxy that transparently forwards MongoDB wire protocol messages between clients and servers.

## Features

- Transparent message forwarding
- Support for MongoDB wire protocol
- Minimal footprint
- Easy to extend

## Installation

```bash
go install github.com/ringsaturn/mangopi/cmd/mangopi@latest
```

## Usage

```bash
# Basic usage
mangopi -listen localhost:27017 -target localhost:27018

# Help
mangopi -help
```

### Command Line Arguments

- `-listen`: Address to listen on (default: localhost:27018)
- `-target`: Target MongoDB address (default: localhost:27017)

## Development

### Building from Source

```bash
git clone https://github.com/ringsaturn/mangopi.git
cd mangopi
go build -o mangopi cmd/mangopi/main.go
```

### Running Tests

```bash
go test ./...
```

## License

MIT License 