# Build stage
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /out/dbfailsim ./cmd/dbfailsim

# Runtime stage - needs psql so the consistency checker's check_command
# (which shells out to psql per node) actually works out of the box.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/dbfailsim /usr/local/bin/dbfailsim
WORKDIR /app
ENTRYPOINT ["dbfailsim"]
