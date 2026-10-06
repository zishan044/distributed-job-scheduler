FROM golang:1.26.3-bookworm AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .

ARG SERVICE
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
ENTRYPOINT ["/service"]
