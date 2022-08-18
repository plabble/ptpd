LDFLAGS    ?= -s -w
BUILDFLAGS ?= -v -trimpath -ldflags '$(LDFLAGS)'
TESTFLAGS  ?= -v
LINTFLAGS  ?=

build: # build project and put the output in bin/
	mkdir -p bin
	go build -o 'bin' $(BUILDFLAGS) ./cmd/...

clean: # remove build related files
	rm -rf bin

test: build # run tests
	go test $(TESTFLAGS) ./...
	golangci-lint run $(LINTFLAGS)

run: # run application
	go run ./cmd/...
