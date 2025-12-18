FROM rust:1.92 as builder

WORKDIR /usr/src/app
COPY . .

RUN cargo build --release --package raft_node

FROM debian:trixie-slim

COPY --from=builder /usr/src/app/target/release/raft_node /usr/local/bin/raft_node

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/raft_node"]