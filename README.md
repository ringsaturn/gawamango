# Gawamango - MongoDB Proxy

<img src="./assets/logo.webp" width="200" alt="Gawamango Logo" align="right">

> [!WARNING]
> This project is not for production use.

Gawamango is a minimal MongoDB proxy that transparently forwards MongoDB wire
protocol messages between clients and servers.

## Features

- Transparent message forwarding
- Support for MongoDB wire protocol
- Minimal footprint
- Easy to extend

## Installation

```bash
go install github.com/ringsaturn/gawamango/cmd/gawamango@latest
```

## Usage

```bash
# Basic usage
gawamango -listen localhost:27017 -target localhost:27018

# Help
gawamango -help
```

For example, start a server:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 gawamango
```

Then use MongoDB client to connect to gawamango.

```py
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27018/")

client.foo.bar.insert_one({"name": "John"})
```

You can view the traces in Jaeger like this:

![](./assets/demo.webp)

### Command Line Arguments

- `-listen`: Address to listen on (default: localhost:27018)
- `-target`: Target MongoDB address (default: localhost:27017)
- `-silent`: Silent mode, disables command logging
- `-production`: Production mode for logger configuration

### Environment Variables

- `OTEL_EXPORTER_TYPE`: OpenTelemetry exporter type to use:
  - `stdout`: Prints trace data directly to console (useful for debugging)
  - `otlp`: Uses OTLP exporter to send trace data to an OpenTelemetry collector (default)

Example of using the stdout exporter for local debugging:

```bash
OTEL_EXPORTER_TYPE=stdout gawamango
```

When using OTLP exporter, you can configure the endpoint using the standard OpenTelemetry environment variables:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 gawamango
```

## Development

### Building from Source

```bash
git clone https://github.com/ringsaturn/gawamango.git
cd gawamango
make build
```

### Running Tests

```bash
make test
```

## License

MIT License
