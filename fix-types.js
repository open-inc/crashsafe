'use strict';
require('dotenv').config();

const { MongoClient, ObjectId } = require('mongodb');

async function fixDatabase() {
  const uri = process.env.OPENINC_MONGO_BACKUP_URI || 'mongodb://localhost:27017';
  // Use DB_DATA from command line args, or fallback to config
  const dbName = process.argv[2] || process.env.OPENINC_MONGO_BACKUP_DB_DATA;

  if (!dbName) {
    console.error('Error: Please provide a database name via OPENINC_MONGO_BACKUP_DB_DATA or as the first argument.');
    process.exit(1);
  }

  console.log(`Connecting to ${uri}...`);
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected.');
    
    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();
    
    let totalFixed = 0;
    
    for (const collInfo of collections) {
      const collName = collInfo.name;
      const coll = db.collection(collName);
      
      console.log(`\nScanning collection: ${collName}`);
      
      // Find all documents and filter locally for string _ids
      // (Querying _id by $type can interact poorly with some MongoDB driver setups)
      const allDocs = await coll.find({}).toArray();
      const corruptedDocs = allDocs.filter(doc => typeof doc._id === 'string');
      
      if (corruptedDocs.length === 0) {
        console.log(`  No corrupted documents found.`);
        continue;
      }
      
      console.log(`  Found ${corruptedDocs.length} documents with string _id. Fixing...`);
      
      let fixedInColl = 0;
      
      for (const doc of corruptedDocs) {
        const oldIdStr = doc._id;
        
        try {
          // 1. Construct proper ObjectId
          const newId = new ObjectId(oldIdStr);
          
          // 2. Clone doc and assign fixed types
          const newDoc = { ...doc, _id: newId };
          
          // Fix basetime
          if (typeof newDoc.basetime === 'string') {
            const time = new Date(newDoc.basetime);
            if (!isNaN(time.getTime())) {
                newDoc.basetime = time;
            }
          }
          
          // Fix updatedAt
          if (typeof newDoc.updatedAt === 'string') {
            const time = new Date(newDoc.updatedAt);
            if (!isNaN(time.getTime())) {
                newDoc.updatedAt = time;
            }
          }
          
          // 3. Insert new document
          await coll.insertOne(newDoc);
          
          // 4. Delete old document (by its exact string matching _id)
          await coll.deleteOne({ _id: oldIdStr });
          
          fixedInColl++;
          totalFixed++;
          
        } catch (err) {
          console.error(`  [!] Failed to fix document with _id ${oldIdStr}:`, err.message);
        }
      }
      
      console.log(`  Fixed ${fixedInColl} documents in ${collName}.`);
    }
    
    console.log(`\nDone! Successfully fixed ${totalFixed} documents database-wide.`);
    
  } catch (err) {
    console.error('Fatal Error:', err);
  } finally {
    await client.close();
    console.log('Disconnected.');
  }
}

fixDatabase().catch(console.error);
