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

  // Insert a mock "corrupted" document — all three typed fields as strings,
  // mimicking a misimport where BSON types collapsed to JSON strings.
  const corruptedDoc = {
    _id: "6f5a3b92c421f8a8b1a3e9c5",
    basetime: "2024-01-01T12:00:00.000Z",
    updatedAt: "2024-01-01T12:30:00.000Z",
    someData: "test",
  };
  await coll.insertOne(corruptedDoc);

  // Run the script against this DB
  console.log(execSync(`node fix-types.js ${dbName}`, { env: { ...process.env, OPENINC_MONGO_BACKUP_URI: uri } }).toString());

  // Verify it fixed types
  const docs = await coll.find().toArray();
  assert.strictEqual(docs.length, 1, 'fix-types should not duplicate or drop documents');
  const doc = docs[0];

  // _id: string → ObjectId
  assert.strictEqual(typeof doc._id, 'object');
  assert.strictEqual(doc._id.constructor.name, 'ObjectId');
  assert.strictEqual(doc._id.toString(), corruptedDoc._id);

  // basetime: string → Date, with the same ISO timestamp
  assert.ok(doc.basetime instanceof Date, 'basetime should be a real BSON Date, not a string');
  assert.strictEqual(doc.basetime.toISOString(), corruptedDoc.basetime, 'basetime must round-trip to the same ISO timestamp');

  // updatedAt: string → Date, with the same ISO timestamp
  assert.ok(doc.updatedAt instanceof Date, 'updatedAt should be a real BSON Date, not a string');
  assert.strictEqual(doc.updatedAt.toISOString(), corruptedDoc.updatedAt, 'updatedAt must round-trip to the same ISO timestamp');

  // Untouched fields stay untouched
  assert.strictEqual(doc.someData, corruptedDoc.someData, 'unrelated fields must be preserved');

  await db.dropDatabase();
  await client.close();
});

test('fix-types.js leaves an unparseable date string as-is', async () => {
  const uri = 'mongodb://localhost:27017';
  const client = new MongoClient(uri);
  await client.connect();
  const dbName = 'test_fix_types_baddate_' + Date.now();
  const db = client.db(dbName);
  const coll = db.collection('test_coll');

  // basetime is a junk string. fix-types.js gates the conversion on
  // !isNaN(time.getTime()) — so it should be preserved as a string, not
  // overwritten with `Invalid Date` or NaN.
  const corruptedDoc = {
    _id: "6f5a3b92c421f8a8b1a3e9c6",
    basetime: "not-a-date",
    updatedAt: "2024-01-01T12:30:00.000Z",
    someData: "test",
  };
  await coll.insertOne(corruptedDoc);

  execSync(`node fix-types.js ${dbName}`, { env: { ...process.env, OPENINC_MONGO_BACKUP_URI: uri } });

  const doc = await coll.findOne({});
  assert.ok(doc, 'document should still exist');
  assert.strictEqual(doc._id.constructor.name, 'ObjectId', '_id should still be fixed');
  assert.strictEqual(typeof doc.basetime, 'string', 'unparseable basetime must NOT be silently replaced with Invalid Date');
  assert.strictEqual(doc.basetime, 'not-a-date');
  assert.ok(doc.updatedAt instanceof Date, 'parseable updatedAt should still be fixed independently');

  await db.dropDatabase();
  await client.close();
});
