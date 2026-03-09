'use strict';
require('dotenv').config();

/**
 * Load and validate all OPENINC_MONGO_BACKUP_* environment variables.
 * Provides lazy access so that commands like 'init' can run without a valid environment.
 */

const PREFIX = 'OPENINC_MONGO_BACKUP_';

function get(key, defaultValue) {
  const val = process.env[`${PREFIX}${key}`];
  if (val === undefined || val === '') {
    return defaultValue;
  }
  return val;
}

function require_env(key) {
  const val = process.env[`${PREFIX}${key}`];
  if (!val) {
    throw new Error(`Missing required environment variable: ${PREFIX}${key}. Run 'init' to create a template.`);
  }
  return val;
}

const config = {
  get uri() { return require_env('URI'); },
  get backupDir() { return get('DIR', './backups'); },
  get cron() { return get('CRON', '0 * * * *'); },

  // DB names — null means "skip this DB"
  get dbData() { return get('DB_DATA', null) || null; },
  get dbParse() { return get('DB_PARSE', null) || null; },

  // Sensor DB options
  get collectionPrefix() { return get('COLLECTION_PREFIX', 'sensors---'); },
  get sensorConfigCollection() { return get('SENSOR_CONFIG_COLLECTION', 'config'); },
  get uiPort() { return parseInt(get('UI_PORT', '3000'), 10); },
  get updatedAtField() { return get('UPDATED_AT_FIELD', 'updatedAt'); },
};

module.exports = config;
