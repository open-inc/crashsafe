'use strict';
const { MongoClient } = require('mongodb');
const config = require('./config');
const logger = require('./logger');
const { redactUri } = require('./uri-redact');

// We support up to two distinct connections: the main backup URI and an
// optional separate restore URI (when OPENINC_MONGO_BACKUP_RESTORE_URI is
// set). Each gets its own pooled MongoClient, cached by URI string, so a
// repeated getDb against the same URI reuses the existing connection.
const clients = new Map(); // uri -> MongoClient

async function connectFor(uri) {
    let client = clients.get(uri);
    if (client) return client;
    logger.debug({ uri: redactUri(uri) }, 'Connecting to MongoDB');
    client = new MongoClient(uri, {
        serverSelectionTimeoutMS: 10_000,
        connectTimeoutMS: 10_000,
    });
    await client.connect();
    clients.set(uri, client);
    logger.info({ uri: redactUri(uri) }, 'Connected to MongoDB');
    return client;
}

/** Backwards-compatible: connect to the main backup URI. */
async function connect() {
    return connectFor(config.uri);
}

async function disconnect() {
    if (clients.size === 0) return;
    const [...all] = clients.values();
    clients.clear();
    for (const c of all) {
        try { await c.close(); }
        catch (err) { logger.warn({ err }, 'Error closing MongoClient'); }
    }
    logger.info('Disconnected from MongoDB');
}

/**
 * Get a Db handle for `dbName` against the optional `uri`. If `uri` is omitted
 * (or null/undefined), the main backup URI from config is used. Backwards
 * compatible with previous single-URI signature.
 *
 * @param {string} dbName
 * @param {string} [uri]   Optional override URI (used by restore to point at
 *                         OPENINC_MONGO_BACKUP_RESTORE_URI).
 */
async function getDb(dbName, uri) {
    const c = await connectFor(uri || config.uri);
    return c.db(dbName);
}

/**
 * Get the underlying MongoClient for `uri` (defaults to the backup URI).
 * Needed by callers that have to issue admin-database commands like
 * `renameCollection` across databases — those don't go through a single
 * Db handle.
 */
async function getClient(uri) {
    return connectFor(uri || config.uri);
}

module.exports = { connect, disconnect, getDb, getClient };
