
# Use Rust image with full toolchain
FROM rust:latest

# Set working directory
WORKDIR /app

# Copy Cargo.toml and Cargo.lock first to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Pre-fetch dependencies
RUN cargo fetch

# Copy the source code
COPY src ./src

# Run tests
CMD ["cargo", "test", "--release"]

