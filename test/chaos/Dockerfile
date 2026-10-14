FROM docker:29.7.2-cli AS docker-cli

FROM golang:1.26.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test -c -race -tags=chaos -o /out/chaos.test ./test/chaos
RUN CGO_ENABLED=0 go build -o /out/test2json cmd/test2json

FROM gcr.io/distroless/base-debian12
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=build /out/chaos.test /out/test2json /
ENTRYPOINT ["/test2json", "-t", "-p", "github.com/zishan044/distributed-job-scheduler/test/chaos", "/chaos.test"]
