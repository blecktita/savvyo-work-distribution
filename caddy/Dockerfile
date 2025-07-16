# caddy/Dockerfile

# 1. Use the builder image which has xcaddy pre‑installed
FROM caddy:2-builder AS builder

# 2. Build Caddy with the DuckDNS plugin
RUN xcaddy build \
    --with github.com/caddy-dns/duckdns

# 3. Switch back to the slim runtime image
FROM caddy:2

# 4. Copy the newly‑built caddy binary into the runtime image
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
