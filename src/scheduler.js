'use strict';
const cron = require('node-cron');
const { runBackup } = require('./backup');
const config = require('./config');
const logger = require('./logger');

let task = null;
let lastRun = null;
let lastStatus = 'idle';

function getStatus() {
    return {
        cron: config.cron,
        lastRun,
        lastStatus,
        running: !!task
    };
}

function start() {
    if (!cron.validate(config.cron)) {
        throw new Error(`Invalid cron expression: "${config.cron}"`);
    }

    logger.info({ schedule: config.cron }, 'Backup scheduler started');

    task = cron.schedule(config.cron, async () => {
        logger.info('Scheduled backup triggered');
        lastRun = new Date();
        lastStatus = 'running';
        try {
            await runBackup({ full: false });
            lastStatus = 'success';
        } catch (err) {
            lastStatus = 'error';
            logger.error({ err }, 'Scheduled backup failed');
        }
    });
}

function stop() {
    if (task) {
        task.stop();
        task = null;
        logger.info('Backup scheduler stopped');
    }
}

module.exports = { start, stop, getStatus };
