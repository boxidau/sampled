FROM golang:1.17-bullseye as build
WORKDIR /build
COPY go.mod go.sum /build/
RUN go mod download
COPY consumer /build/consumer/
RUN go build -o sampled-consumer ./consumer

FROM debian:bullseye
WORKDIR /
COPY --from=build /build/sampled-consumer /
COPY config.example.yaml /config.yaml

# prometheus metrics default port
EXPOSE 9100/tcp
ENTRYPOINT ["/sampled-consumer", "-logtostderr"]