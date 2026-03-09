'use strict';
const pino = require('pino');

const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    transport: process.stdout.isTTY
        ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:standard' } }
        : undefined,
});

module.exports = logger;
