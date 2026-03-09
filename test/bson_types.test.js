const { test } = require('node:test');
const assert = require('node:assert');
const { MongoClient, ObjectId } = require('mongodb');
const fs = require('node:fs');
const path = require('node:path');
const config = require('../src/config');
const { runBackup } = require('../src/backup');
const { runRestore } = require('../src/restore');

test('BSON types persist through backup and restore', async () => {
    // Setup test DB
    const uri = process.env.OPENINC_MONGO_BACKUP_URI || 'mongodb://localhost:27017';
    const client = new MongoClient(uri);
    await client.connect();
    const dbName = 'test_backup_bson_' + Date.now();
    const db = client.db(dbName);
    const coll = db.collection('sensors---test');
    
    // Override config for testing
    Object.defineProperty(config, 'dbData', { get: () => dbName, configurable: true });
    Object.defineProperty(config, 'backupDir', { get: () => path.join(__dirname, 'temp_backups'), configurable: true });
    Object.defineProperty(config, 'collectionPrefix', { get: () => 'sensors---', configurable: true });
    
    // Clean any old backups
    fs.rmSync(config.backupDir, { recursive: true, force: true });

    // Insert a document with BSON types
    const originalDoc = {
        _id: new ObjectId(),
        sensorId: 'sensor-1',
        basetime: new Date('2024-01-01T12:00:00Z'),
        updatedAt: new Date(),
        nested: {
            customId: new ObjectId()
        }
    };
    await coll.insertOne(originalDoc);

    // 1. Run backup
    await runBackup({ full: true });

    // 2. Clear collection
    await coll.deleteMany({});
    let count = await coll.countDocuments();
    assert.strictEqual(count, 0, 'Collection should be empty before restore');

    // 3. Run restore
    await runRestore('data', null, true, null, false);

    // 4. Verify document and its types
    const restoredDoc = await coll.findOne({ _id: originalDoc._id });
    
    assert.ok(restoredDoc, 'Document should be restored');
    assert.ok(restoredDoc._id instanceof ObjectId, '_id should be an ObjectId');
    assert.ok(restoredDoc.basetime instanceof Date, 'basetime should be a Date');
    assert.ok(restoredDoc.nested.customId instanceof ObjectId, 'nested customId should be an ObjectId');
    
    assert.strictEqual(restoredDoc._id.toString(), originalDoc._id.toString());
    assert.strictEqual(restoredDoc.basetime.toISOString(), originalDoc.basetime.toISOString());
    assert.strictEqual(restoredDoc.nested.customId.toString(), originalDoc.nested.customId.toString());

    // Cleanup
    await db.dropDatabase();
    await client.close();
    fs.rmSync(config.backupDir, { recursive: true, force: true });
});
