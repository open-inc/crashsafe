FROM node:current-alpine


# Install MongoDB CLI Tools (mongodump, mongorestore)
RUN apk add --no-cache mongodb-tools

WORKDIR /app

# Install Node.js dependencies
COPY package*.json ./
RUN npm install --omit=dev

# Bundle app source
COPY src ./src
COPY public ./public

# Ensure src/index.js is executable if necessary, though it is usually invoked via node
RUN chmod +x src/index.js

# Entrypoint configures node to run the CLI standard tool properly
ENTRYPOINT ["node", "src/index.js"]

# Default command if none is supplied
CMD ["--help"]
