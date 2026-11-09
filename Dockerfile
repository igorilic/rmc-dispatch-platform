# One image per binary: docker build --build-arg BIN=simulators .
FROM rust:1.99.1-slim-trixie AS build
ARG BIN
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin "$BIN" && cp "target/release/$BIN" /app

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /app /app
ENTRYPOINT ["/app"]
