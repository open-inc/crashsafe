'use strict';
const cron = require('node-cron');
const { runBackup } = require('./backup');
const { runVerify } = require('./verify');
const config = require('./config');
const logger = require('./logger');

// --- Backup scheduler (always on) -------------------------------------------

let backupTask = null;
let backupLastRun = null;
let backupLastStatus = 'idle';

function getStatus() {
    return {
        cron: config.cron,
        lastRun: backupLastRun,
        lastStatus: backupLastStatus,
        running: !!backupTask,
    };
}

// --- Verify scheduler (only on when OPENINC_MONGO_BACKUP_VERIFY_CRON is set)
// Catches bit-rot / silent corruption that the on-demand verify button will
// otherwise never see, because in production no operator presses it. Logs at
// error level on any corruption so external log-based alerting can match.

let verifyTask = null;
let verifyLastRun = null;
let verifyLastStatus = 'idle';
let verifyLastSummary = null;

function getVerifyStatus() {
    if (!config.verifyCron) return null;
    return {
        cron: config.verifyCron,
        deep: config.verifyDeep,
        lastRun: verifyLastRun,
        lastStatus: verifyLastStatus,
        lastSummary: verifyLastSummary,
        running: !!verifyTask,
    };
}

function start() {
    // Backup cron
    if (!cron.validate(config.cron)) {
        throw new Error(`Invalid cron expression: "${config.cron}"`);
    }
    logger.info({ schedule: config.cron }, 'Backup scheduler started');
    backupTask = cron.schedule(config.cron, async () => {
        logger.info('Scheduled backup triggered');
        backupLastRun = new Date();
        backupLastStatus = 'running';
        try {
            const result = await runBackup({ full: false, trigger: 'scheduled' });
            if (result?.skipped) {
                backupLastStatus = 'skipped';
                logger.warn({ reason: result.reason, holder: result.holder }, 'Scheduled backup skipped (another run in progress)');
            } else {
                backupLastStatus = 'success';
            }
        } catch (err) {
            backupLastStatus = 'error';
            logger.error({ err }, 'Scheduled backup failed');
        }
    });

    // Verify cron — optional. Empty / unset env var → never schedule.
    if (config.verifyCron) {
        if (!cron.validate(config.verifyCron)) {
            throw new Error(`Invalid verify cron expression: "${config.verifyCron}"`);
        }
        logger.info({ schedule: config.verifyCron, deep: config.verifyDeep }, 'Verify scheduler started');
        verifyTask = cron.schedule(config.verifyCron, async () => {
            logger.info({ deep: config.verifyDeep }, 'Scheduled verify triggered');
            verifyLastRun = new Date();
            verifyLastStatus = 'running';
            try {
                const result = await runVerify({
                    target: 'all',
                    deep: config.verifyDeep,
                    trigger: 'scheduled',
                });
                if (result?.skipped) {
                    verifyLastStatus = 'skipped';
                    verifyLastSummary = null;
                    logger.warn({ reason: result.reason, holder: result.holder }, 'Scheduled verify skipped (another operation in progress)');
                    return;
                }
                verifyLastSummary = result.summary;
                if (result.summary.corrupt > 0 || result.summary.manifestErrors > 0) {
                    // Loud log on real corruption — meant for log-based alerting.
                    // Sample the first few details so operators don't need to grep
                    // everywhere; the full report is still in the on-disk verify log.
                    const sampleDetails = (result.details || [])
                        .filter((d) => d.status === 'corrupt' || d.status === 'manifest-error')
                        .slice(0, 5)
                        .map((d) => ({ dbType: d.dbType, entryId: d.entryId, status: d.status, issueCount: d.issues?.length ?? 0 }));
                    verifyLastStatus = 'failure';
                    logger.error({ summary: result.summary, sampleDetails }, 'Scheduled verify found corruption');
                } else if (result.summary.noBaseline > 0) {
                    verifyLastStatus = 'warnings';
                    logger.warn({ summary: result.summary }, 'Scheduled verify completed with legacy entries (no baseline)');
                } else {
                    verifyLastStatus = 'success';
                    logger.info({ summary: result.summary }, 'Scheduled verify completed clean');
                }
            } catch (err) {
                verifyLastStatus = 'error';
                logger.error({ err }, 'Scheduled verify failed');
            }
        });
    }
}

function stop() {
    if (backupTask) {
        backupTask.stop();
        backupTask = null;
        logger.info('Backup scheduler stopped');
    }
    if (verifyTask) {
        verifyTask.stop();
        verifyTask = null;
        logger.info('Verify scheduler stopped');
    }
}

module.exports = { start, stop, getStatus, getVerifyStatus };
