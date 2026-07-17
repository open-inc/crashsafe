# --- mongodb-tools build stage ------------------------------------------------
# Every prebuilt mongodump/mongorestore (Alpine's mongodb-tools 100.14.1-r3,
# upstream releases through 100.17.0) still embeds golang.org/x/crypto < 0.52.0
# (CVE-2026-46595 et al.) and golang.org/x/net < 0.55.0 (CVE-2026-39821), so
# scanners flag the image no matter which package we install. Build the two
# tools we actually ship from the latest release tag with those modules bumped
# to the fixed versions (upstream master already uses them). Drop this stage
# and return to `apk add mongodb-tools` once a fixed package ships.
FROM golang:1.25-alpine AS mongo-tools
RUN apk add --no-cache git
ARG MONGO_TOOLS_VERSION=100.17.0
RUN git clone --depth 1 --branch ${MONGO_TOOLS_VERSION} https://github.com/mongodb/mongo-tools.git /src
WORKDIR /src
# The repo vendors its dependencies, so re-vendor after the bump.
RUN go get golang.org/x/crypto@v0.53.0 golang.org/x/net@v0.56.0 golang.org/x/text@v0.40.0 \
    && go mod vendor
# -X main.VersionStr mirrors what upstream's release build injects, so
# `mongodump --version` stays meaningful.
RUN CGO_ENABLED=0 go build -trimpath \
        -ldflags="-s -w -X main.VersionStr=${MONGO_TOOLS_VERSION} -X main.GitCommit=${MONGO_TOOLS_VERSION}" \
        -o /out/mongodump ./mongodump/main \
    && CGO_ENABLED=0 go build -trimpath \
        -ldflags="-s -w -X main.VersionStr=${MONGO_TOOLS_VERSION} -X main.GitCommit=${MONGO_TOOLS_VERSION}" \
        -o /out/mongorestore ./mongorestore/main

# --- runtime image --------------------------------------------------------------
# node:current tracks the latest Node.js release line, which can ship breaking
# changes; node:lts pins to the active LTS line for a more stable production base.
FROM node:lts-alpine

# Pull the latest Alpine security patches at build time, then install
# util-linux (provides `ionice`, needed when
# OPENINC_MONGO_BACKUP_NICE_BACKUP=true wraps mongodump in
# `nice -n 19 ionice -c 3 …`). `nice` itself ships with busybox so no extra
# package is needed for the CPU-priority side. Rebuild the image periodically
# to pick up new CVE fixes from the upstream Alpine repos.
RUN apk update \
    && apk upgrade --no-cache \
    && apk add --no-cache util-linux

# mongodump/mongorestore built above with patched Go modules.
COPY --from=mongo-tools /out/mongodump /out/mongorestore /usr/local/bin/

# The node base image bundles an npm whose vendored dependencies (e.g. undici)
# can lag behind security fixes; upgrade npm itself so its bundled deps are
# current too.
RUN npm install -g npm@latest

WORKDIR /app

# Install Node.js dependencies
COPY package*.json ./
RUN npm install --omit=dev

# Bundle app source
COPY src ./src
COPY public ./public

# Ensure src/index.js is executable if necessary, though it is usually invoked via node
RUN chmod +x src/index.js

# Entrypoint configures node to run the CLI standard tool properly.
# --max-old-space-size caps the V8 heap so a runaway backup OOMs Node cleanly
# instead of the Linux OOM killer choosing mongod.
ENTRYPOINT ["node", "--max-old-space-size=1024", "src/index.js"]

# Default command if none is supplied
CMD ["--help"]
