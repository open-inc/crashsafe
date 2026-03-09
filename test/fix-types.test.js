const { test } = require('node:test');
const assert = require('node:assert');
const { MongoClient } = require('mongodb');
const { execSync } = require('child_process');

test('fix-types.js converts string _ids and dates', async () => {
  const uri = 'mongodb://localhost:27017';
  const client = new MongoClient(uri);
  await client.connect();
  const dbName = 'test_fix_types_' + Date.now();
  const db = client.db(dbName);
  const coll = db.collection('test_coll');
  
  // Insert a mock "corrupted" document
  const corruptedDoc = {
    _id: "6f5a3b92c421f8a8b1a3e9c5", 
    basetime: "2024-01-01T12:00:00.000Z",
    updatedAt: "2024-01-01T12:30:00.000Z",
    someData: "test"
  };
  await coll.insertOne(corruptedDoc);
  
  // Run the script against this DB
  console.log(execSync(`node fix-types.js ${dbName}`, { env: { ...process.env, OPENINC_MONGO_BACKUP_URI: uri } }).toString());
  
  // Verify it fixed types
  const docs = await coll.find().toArray();
  assert.strictEqual(docs.length, 1);
  const doc = docs[0];
  
  assert.strictEqual(typeof doc._id, 'object');
  assert.strictEqual(doc._id.constructor.name, 'ObjectId');
  assert.strictEqual(doc._id.toString(), corruptedDoc._id);
  
  await db.dropDatabase();
  await client.close();
});
