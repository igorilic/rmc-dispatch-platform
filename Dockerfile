# One image per binary: docker build --build-arg BIN=simulators .
FROM rust:1.99.1-slim-trixie AS build
ARG BIN
WORKDIR /src
COPY . .
# The plant-agent is its own workspace (crates/plant-agent/Cargo.toml says why); both build into
# /src/target, as .cargo/config.toml sets.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    manifest=Cargo.toml; [ "$BIN" = plant-agent ] && manifest=crates/plant-agent/Cargo.toml; \
    cargo build --release --locked --manifest-path "$manifest" --bin "$BIN" && cp "target/release/$BIN" /app

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /app /app
ENTRYPOINT ["/app"]
