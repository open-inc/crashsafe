'use strict';
const { MongoClient } = require('mongodb');
const config = require('./config');
const logger = require('./logger');

/** @type {MongoClient|null} */
let client = null;

async function connect() {
    if (client) return client;
    logger.debug({ uri: config.uri }, 'Connecting to MongoDB');
    client = new MongoClient(config.uri, {
        serverSelectionTimeoutMS: 10_000,
        connectTimeoutMS: 10_000,
    });
    await client.connect();
    logger.info('Connected to MongoDB');
    return client;
}

async function disconnect() {
    if (!client) return;
    await client.close();
    client = null;
    logger.info('Disconnected from MongoDB');
}

/**
 * Get a db handle for the given database name.
 * @param {string} dbName
 */
async function getDb(dbName) {
    const c = await connect();
    return c.db(dbName);
}

module.exports = { connect, disconnect, getDb };
