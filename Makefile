build:
	mkdir -p bin
	go build -o bin/gawamango cmd/gawamango/main.go

run:
	go run cmd/gawamango/main.go > run.log 2>&1

clean:
	rm -rf bin

test:
	go test -v ./...
