FROM golang:1.26.3-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG SERVICE
ARG PACKAGE=./cmd/${SERVICE}
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service "${PACKAGE}"

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
ENTRYPOINT ["/service"]
