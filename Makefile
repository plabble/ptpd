build: # build project and put the output in bin/
	mkdir -p bin
	go build -v -o 'bin' -ldflags '-s -w' -trimpath ./cmd/...

clean: # remove build related files
	rm -rf bin

test: build # run tests
	go test -v ./...
	golangci-lint run

run: # run application
	go run ./cmd/...
