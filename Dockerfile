# --- mongodb-tools build stage ------------------------------------------------
# Every prebuilt mongodump/mongorestore (Alpine's mongodb-tools 100.14.1-r3,
# upstream releases through 100.17.0) still embeds golang.org/x/crypto < 0.55.0
# (CVE-2026-56854 et al.), golang.org/x/net < 0.55.0 (CVE-2026-39821) and
# github.com/klauspost/compress < 1.18.7 (GHSA-259r-337f-4rfw), so scanners
# flag the image no matter which package we install. Build the two tools we
# actually ship from the latest release tag with those modules bumped to the
# fixed versions. Drop this stage and return to `apk add mongodb-tools` once a
# fixed package ships.
#
# GO-2026-5932 (golang.org/x/crypto/openpgp is unmaintained) has no fixed
# version, so it stays reported for as long as x/crypto is a dependency at
# all. Nothing here imports openpgp -- `go mod vendor` does not pull it in and
# it is absent from both binaries -- so the report is module-level noise.
# Pin the patch release explicitly: the floating 1.25 tag let an older
# toolchain get baked in, and Go stdlib < 1.25.13 is flagged for
# CVE-2026-39821 (net/http's vendored x/net/idna), CVE-2026-33818
# (encoding/asn1), CVE-2026-56859 (encoding/xml), CVE-2026-56853
# (net/http) and CVE-2026-56862 (crypto/tls). Bump this when a newer
# patch release lands.
FROM golang:1.25.14-alpine AS mongo-tools
RUN apk add --no-cache git
ARG MONGO_TOOLS_VERSION=100.17.0
RUN git clone --depth 1 --branch ${MONGO_TOOLS_VERSION} https://github.com/mongodb/mongo-tools.git /src
WORKDIR /src
# The repo vendors its dependencies, so re-vendor after the bump.
RUN go get golang.org/x/crypto@v0.55.0 golang.org/x/net@v0.57.0 golang.org/x/text@v0.41.0 \
        github.com/klauspost/compress@v1.18.7 \
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

WORKDIR /app

# Install Node.js dependencies, then strip the package managers out of the
# runtime image. npm vendors its own dependency tree and even npm@latest still
# bundles ip-address 10.2.0 (CVE-2026-69192), so upgrading npm cannot clear it
# -- and nothing at runtime shells out to npm/npx/yarn, only to
# mongodump/mongorestore. Removing them in the same layer as the install keeps
# them out of the final image entirely. Keep this combined: splitting the rm
# into its own RUN leaves the files in an earlier layer.
COPY package*.json ./
RUN npm install --omit=dev \
    && rm -rf /usr/local/lib/node_modules/npm \
              /usr/local/lib/node_modules/corepack \
              /usr/local/bin/npm \
              /usr/local/bin/npx \
              /usr/local/bin/corepack \
              /usr/local/bin/yarn \
              /usr/local/bin/yarnpkg \
              /opt/yarn-v* \
              /root/.npm

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
