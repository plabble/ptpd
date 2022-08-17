FROM golang:1.19-alpine as build

# setup enviroment
WORKDIR /src
ENV CGO_ENABLED=0
RUN apk add --no-cache make && \
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.48

# download modules
COPY go.* .
RUN go mod download

# build and test
COPY . .
RUN make build test



FROM gcr.io/distroless/static-debian11
COPY --from=build /src/bin/ptpd /
CMD ["/ptpd"]
