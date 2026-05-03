#!/usr/bin/env node
'use strict';
const scheduler = require('./scheduler');
const server = require('./server');
const logger = require('./logger');

// If any CLI subcommands are passed, delegate to the CLI parser
const args = process.argv.slice(2);
const CLI_COMMANDS = ['backup', 'restore', 'list', 'verify', 'init', '--help', '-h', '--version', '-V'];

if (args.length && CLI_COMMANDS.some((cmd) => args[0] === cmd || args[0].startsWith('-'))) {
    // CLI mode — load and parse
    const program = require('./cli');
    program.parseAsync(process.argv).catch((err) => {
        logger.error({ err }, 'CLI error');
        process.exit(1);
    });
} else {
    // Daemon mode — start scheduler and UI server
    logger.info('Starting OpenInc Backup Daemon');

    // Restore dashboard state from on-disk manifests (sizes + lastRuns) before
    // the UI comes up so /api/status doesn't briefly serve empty data.
    const { runStartupTasks } = require('./backup');
    try {
        runStartupTasks();
    } catch (err) {
        logger.warn({ err }, 'Startup tasks failed; continuing anyway');
    }

    scheduler.start();
    server.start();

    const shutdown = async (signal) => {
        logger.info({ signal }, 'Shutting down...');
        scheduler.stop();
        server.stop();
        const { disconnect } = require('./db');
        await disconnect();
        process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}
