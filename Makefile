build:
	mkdir -p bin
	go build -o bin/gawamango cmd/gawamango/main.go

run:
	go run cmd/gawamango/main.go

clean:
	rm -rf bin

test:
	go test -v ./...
