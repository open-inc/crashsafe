# node:current tracks the latest Node.js release line, which can ship breaking
# changes; node:lts pins to the active LTS line for a more stable production base.
FROM node:lts-alpine

# Pull the latest Alpine security patches at build time, then install
# MongoDB CLI tools (mongodump, mongorestore). Rebuild the image periodically
# to pick up new CVE fixes from the upstream Alpine repos.
RUN apk update \
    && apk upgrade --no-cache \
    && apk add --no-cache mongodb-tools

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
