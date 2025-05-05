build:
	mkdir -p bin
	go build -o bin/mangopi cmd/mangopi/main.go

run:
	go run cmd/mangopi/main.go

clean:
	rm -rf bin
