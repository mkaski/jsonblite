import fs from 'fs';
import assert from 'assert';
import { encode } from 'cbor-x';
import Jsonblite from '../src/jsonblite.ts';
import {
    HEADER_DATA_SIZE_BYTES,
    HEADER_SIZE,
    LOCATION_DATA_SIZE,
    LOCATION_INDEX_SIZE,
} from '../src/constants.ts';

const DB_FILE = './data/acid.jsonblite';
const JOURNAL_FILE = `${DB_FILE}.journal`;
const TEMP_FILE = `${DB_FILE}.temp`;

function cleanupFiles() {
    if (fs.existsSync(DB_FILE)) {
        fs.unlinkSync(DB_FILE);
    }
    if (fs.existsSync(JOURNAL_FILE)) {
        fs.unlinkSync(JOURNAL_FILE);
    }
    if (fs.existsSync(TEMP_FILE)) {
        fs.unlinkSync(TEMP_FILE);
    }
}

function atomicity() {
    const db = new Jsonblite(DB_FILE);
    db.write('stable', { value: 1 });

    // Build a valid "in-flight" transaction journal and verify constructor recovery.
    const fd = fs.openSync(DB_FILE, 'r');
    const header = Buffer.alloc(HEADER_SIZE);
    fs.readSync(fd, header, 0, HEADER_SIZE, 0);
    const dataSize = header.readUIntLE(LOCATION_DATA_SIZE, HEADER_DATA_SIZE_BYTES);
    const indexSize = header.readUInt32LE(LOCATION_INDEX_SIZE);
    const dataOffset = HEADER_SIZE + dataSize;
    const index = Buffer.alloc(indexSize);
    fs.readSync(fd, index, 0, indexSize, dataOffset);
    fs.closeSync(fd);

    const replayTransaction = {
        key: 'noop',
        operation: 'delete',
        data: null,
        index,
        header,
        dataOffset,
    };
    fs.writeFileSync(JOURNAL_FILE, encode(replayTransaction));
    assert.strictEqual(fs.existsSync(JOURNAL_FILE), true, 'Journal fixture should exist');

    const recovered = new Jsonblite(DB_FILE);
    assert.deepStrictEqual(recovered.read('stable'), { value: 1 }, 'Recovery should preserve committed data');
    assert.strictEqual(fs.existsSync(JOURNAL_FILE), false, 'Recovery should clear journal');
}

function consistency() {
    const db = new Jsonblite(DB_FILE);
    assert.throws(() => db.write('', { value: 1 }), 'Invalid key should throw error');
    assert.throws(() => db.read(''), 'Invalid key should throw error');
    assert.throws(() => db.delete(''), 'Invalid key should throw error');

    db.write('key1', { value: 1 });
    assert.deepStrictEqual(db.read('key1'), { value: 1 }, 'Read value should match written value');
}

function isolation() {
    const db1 = new Jsonblite(DB_FILE);
    const db2 = new Jsonblite(DB_FILE);
    const db3 = new Jsonblite(DB_FILE);
    const db4 = new Jsonblite(DB_FILE);
    const dbs = [db1, db2, db3, db4];

    for (let i = 0; i < 200; i++) {
        dbs[i % dbs.length].write(`k${i}`, { value: i });
    }

    const expectedKeys = db1.keys().sort();
    for (const db of dbs) {
        assert.deepStrictEqual(db.keys().sort(), expectedKeys, 'All instances should observe the same keys');
    }

    for (let i = 0; i < 200; i++) {
        const key = `k${i}`;
        assert.deepStrictEqual(db2.read(key), { value: i }, `Value mismatch for key ${key}`);
        assert.deepStrictEqual(db3.read(key), { value: i }, `Value mismatch for key ${key}`);
    }

    db1.write('shared', { value: 1 });
    db2.write('shared', { value: 2 });
    assert.deepStrictEqual(db3.read('shared'), { value: 2 }, 'Latest write should be visible across instances');

    db4.delete('shared');
    assert.strictEqual(db1.read('shared'), null, 'Delete should be visible across instances');
    assert.strictEqual(db2.read('shared'), null, 'Delete should be visible across instances');

    db2.delete('k10');
    assert.strictEqual(db3.read('k10'), null, 'Delete should remove key for all instances');
}

function durability() {
    let db: Jsonblite | null = new Jsonblite(DB_FILE);
    db.write('durable', { value: 1 });
    db = null;

    const db2 = new Jsonblite(DB_FILE);
    assert.deepStrictEqual(db2.read('durable'), { value: 1 }, 'Data should persist after restart');
}

async function test() {
    cleanupFiles();

    console.log('Testing Atomicity...');
    atomicity();

    console.log('Testing Consistency...');
    consistency();

    console.log('Testing Isolation...');
    isolation();

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
        process.exitCode = 1;
    })
    .finally(() => {
        cleanupFiles();
    });
