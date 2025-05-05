# gawamango - MongoDB Proxy

> [!WARNING]
> This project is not for production use.

gawamango is a minimal MongoDB proxy that transparently forwards MongoDB wire
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
go run cmd/gawamango/main.go
```

Then use MongoDB client to connect to gawamango.

```py
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27018/")

client.foo.bar.insert_one({"name": "John"})
```

<details>
<summary>

The MongoPi will output some logs like this:

</summary>

```console
MongoDB Command:
  Database:
  Command: helloOk
  Arguments: {
  "command": "helloOk",
  "database": "",
  "arguments": {
    "collection": "admin.$cmd",
    "helloOk": true
  }
}
----------------------------------------
MongoDB Command:
  Database: admin
  Command: hello
  Arguments: {
  "command": "hello",
  "database": "admin",
  "arguments": {
    "hello": 1
  }
}
----------------------------------------
MongoDB Command:
  Database:
  Command: client
  Arguments: {
  "command": "client",
  "database": "",
  "arguments": {
    "client": {
      "driver": {
        "name": "PyMongo|c",
        "version": "4.12.1"
      },
      "os": {
        "architecture": "arm64",
        "name": "Darwin",
        "type": "Darwin",
        "version": "15.4.1"
      },
      "platform": "CPython 3.13.3.final.0"
    },
    "collection": "admin.$cmd"
  }
}
----------------------------------------
MongoDB Command:
  Database:
  Command: ismaster
  Arguments: {
  "command": "ismaster",
  "database": "",
  "arguments": {
    "collection": "admin.$cmd",
    "ismaster": 1
  }
}
----------------------------------------
MongoDB Command:
  Database: foo
  Command: insert
  Arguments: {
  "command": "insert",
  "database": "foo",
  "arguments": {
    "insert": "bar"
  }
}
----------------------------------------
```

</details>

### Command Line Arguments

- `-listen`: Address to listen on (default: localhost:27018)
- `-target`: Target MongoDB address (default: localhost:27017)

## Development

### Building from Source

```bash
git clone https://github.com/ringsaturn/gawamango.git
cd gawamango
go build -o gawamango cmd/gawamango/main.go
```

### Running Tests

```bash
go test ./...
```

## License

MIT License
