FROM golang:1.19-alpine as build

# setup enviroment
WORKDIR /src
ENV CGO_ENABLED=1 \
	CC=x86_64-alpine-linux-musl-gcc \
	LDFLAGS="-s -w -linkmode=external -extldflags=-static"
RUN apk add --no-cache make gcc musl-dev && \
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.48

# download modules
COPY go.* ./
RUN go mod download

# build and test
COPY . .
RUN make build test



FROM gcr.io/distroless/static-debian11
COPY --from=build /src/bin/ptpd /
CMD ["/ptpd"]
