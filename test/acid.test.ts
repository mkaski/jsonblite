import fs from 'fs';
import assert from 'assert';
import Jsonblite from '../src/jsonblite.js';

const DB_FILE = './acid.jsonblite';

async function atomicity() {
    const db = new Jsonblite(DB_FILE);
    // TODO: Test journaling and recovery in case of crash
}

function consistency() {
    const db = new Jsonblite(DB_FILE);
    assert.throws(() => db.write('', { value: 1 }), 'Invalid key should throw error');
    db.write('key1', { value: 1 });
    assert.deepStrictEqual(db.read('key1'), { value: 1 }, 'Read value should match written value');
}

async function isolation() {
    // Create database instances of the same db file.
    const db1 = new Jsonblite(DB_FILE);
    const db2 = new Jsonblite(DB_FILE);
    const db3 = new Jsonblite(DB_FILE);
    const db4 = new Jsonblite(DB_FILE);

    // Create 100 database instances of the same db file.
    const dbs = Array.from({ length: 100 }, () => new Jsonblite(DB_FILE, { verbose: false }));

    // Collect all write promises
    const writePromises = dbs.map(async (db) => {
        const writeKey = Math.random().toString(36).substring(7);
        db.write(writeKey, { value: 1 });
    });

    // Wait for all writes to complete
    await Promise.all(writePromises);

    // Check that all databases have the same keys
    for (const db of dbs) {
        assert.deepStrictEqual(db.keys(), dbs[0].keys(), 'All databases should have the same keys');
    }


    // Write to each database instance concurrently.
    await Promise.all([
        db1.write('key1', { value: 1 }),
        db1.write('key2', { value: 222 }),
        db1.write('key2', { value: 222 }),
        db2.write('key2', { value: 2 }),
        db3.write('key3', { value: 3 }),
        db4.write('key4', { value: 4 }),
    ]);

    console.log('db1', db1.keys());
    console.log('db2', db2.keys());
    console.log('db3', db3.keys());
    console.log('db4', db4.keys());

    // All databases should have the same keys and values.
    assert.deepStrictEqual(db1.keys(), db2.keys(), 'All databases should have the same keys');
    assert.deepStrictEqual(db1.keys(), db3.keys(), 'All databases should have the same keys');
    assert.deepStrictEqual(db2.keys(), db3.keys(), 'All databases should have the same keys');
    assert.deepStrictEqual(db1.keys(), db4.keys(), 'All databases should have the same keys');

    assert.deepStrictEqual(db1.read('key1'), { value: 1 }, 'DB1 should read its own written value');
    assert.deepStrictEqual(db1.read('key2'), { value: 2 }, 'DB2 should read its own written value');
    assert.deepStrictEqual(db1.read('key3'), { value: 3 }, 'DB3 should read its own written value');
    assert.deepStrictEqual(db1.read('key4'), { value: 4 }, 'DB4 should read its own written value');

    db1.delete('key1');
    db2.delete('key2');
    db3.delete('key3');

    console.log('db1', db1.keys());
    console.log('db2', db2.keys());
    console.log('db3', db3.keys());

    // TODO: This is not consistent. There is some race condition in the lock mechanism.
    await Promise.all([
        db1.delete('key1'),
        db2.write('key2', { value: 22 }),
        db3.write('key3', { value: 33 }),
        db4.delete('key4'),
    ]);

    assert.strictEqual(db1.read('key1'), null, 'Key1 should be deleted');
    assert.deepStrictEqual(db2.read('key3'), { value: 33 }, 'DB2 should read DB3 written value');
    assert.deepStrictEqual(db3.read('key2'), { value: 22 }, 'DB3 should read DB2 written value');
    assert.deepStrictEqual(db4.read('key4'), null, 'Key4 should be deleted');
}

function durability() {
    let db: Jsonblite | null = new Jsonblite(DB_FILE);
    db.write('key1', { value: 1 });
    db = null; // Close the database

    // Re-initialize from file
    const db2 = new Jsonblite(DB_FILE);
    assert.deepStrictEqual(db2.read('key1'), { value: 1 }, 'Data should persist after restart');
}

// Run all tests
async function test() {
    // Cleanup
    if (fs.existsSync(DB_FILE)) {
        fs.unlinkSync(DB_FILE);
    }

    console.log('Testing Atomicity...');
    await atomicity();

    console.log('Testing Consistency...');
    consistency();

    console.log('Testing Isolation...');
    await isolation();

    console.log('Testing Durability...');
    durability();
}

test()
    .then(() => {
        console.log('✅ All tests passed successfully');
    })
    .catch(err => {
        console.trace(err);
        console.error('❌ An error occurred during tests:', err.message);
    })
    .finally(() => {
        // Cleanup
        if (fs.existsSync(DB_FILE)) {
            fs.unlinkSync(DB_FILE);
        }
    });
