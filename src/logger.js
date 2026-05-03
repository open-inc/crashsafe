'use strict';
const pino = require('pino');
const { redactErr, redactUri } = require('./uri-redact');

// Fail-closed default: any `err`, `error`, `uri`, `cmd` field passed to a log
// call is run through the URI redactor before pino sees it. Without this,
// catch-blocks elsewhere in the codebase that log `{ err }` would leak the
// Mongo password the moment the err comes from execFile (mongodump/mongorestore)
// or from any future code path that hasn't been individually patched.
const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    transport: process.stdout.isTTY
        ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:standard' } }
        : undefined,
    serializers: {
        err: redactErr,
        error: redactErr,
        uri: redactUri,
        cmd: redactUri,
        stderr: redactUri,
        stdout: redactUri,
    },
});

module.exports = logger;
