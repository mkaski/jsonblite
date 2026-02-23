import fs from 'fs';
import assert from 'assert';
import JSONBLite from '../src/jsonblite.ts';

const DB_FILE = './data/example.jsonblite';
const DUMP_FILE = './data/example.json';
const JOURNAL_FILE = `${DB_FILE}.journal`;
const TEMP_FILE = `${DB_FILE}.temp`;

function cleanupFiles() {
    if (fs.existsSync(DB_FILE)) {
        fs.unlinkSync(DB_FILE);
    }
    if (fs.existsSync(DUMP_FILE)) {
        fs.unlinkSync(DUMP_FILE);
    }
    if (fs.existsSync(JOURNAL_FILE)) {
        fs.unlinkSync(JOURNAL_FILE);
    }
    if (fs.existsSync(TEMP_FILE)) {
        fs.unlinkSync(TEMP_FILE);
    }
}

function streamToString(stream: NodeJS.ReadableStream): Promise<string> {
    return new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        stream.on('data', (chunk) => {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
        stream.on('error', reject);
    });
}

function testBasicOperations() {
    const db = new JSONBLite(DB_FILE);
    const createdAt = new Date('2024-08-04T12:00:00.000Z');

    db.write('string', 'value');
    db.write('number', 123);
    db.write('object', { nested: { ok: true } });
    db.write('array', [1, 2, 3]);
    db.write('boolean', true);
    db.write('null', null);
    db.write('undefined', undefined);
    db.write('date', createdAt);
    db.write('infinity', Infinity);
    db.write('nan', NaN);

    assert.strictEqual(db.read('missing'), null, 'Missing keys should return null');
    assert.strictEqual(db.read('string'), 'value');
    assert.strictEqual(db.read('number'), 123);
    assert.deepStrictEqual(db.read('object'), { nested: { ok: true } });
    assert.deepStrictEqual(db.read('array'), [1, 2, 3]);
    assert.strictEqual(db.read('boolean'), true);
    assert.strictEqual(db.read('null'), null);
    assert.strictEqual(db.read('undefined'), undefined);
    assert.strictEqual((db.read('date') as Date).toISOString(), createdAt.toISOString());
    assert.strictEqual(db.read('infinity'), Infinity);
    assert.strictEqual(Number.isNaN(db.read('nan')), true, 'NaN should round-trip as NaN');

    db.write('string', 'updated');
    assert.strictEqual(db.read('string'), 'updated', 'Latest write should replace previous value');

    db.delete('array');
    assert.strictEqual(db.read('array'), null, 'Deleted key should return null');

    const keys = db.keys();
    assert.strictEqual(keys.includes('string'), true);
    assert.strictEqual(keys.includes('array'), false);
}

function testMultipleInstancesAndPersistence() {
    const db1 = new JSONBLite(DB_FILE);
    const db2 = new JSONBLite(DB_FILE);

    db1.write('shared', { count: 1 });
    assert.deepStrictEqual(db2.read('shared'), { count: 1 }, 'Instances should observe each other writes');

    db2.write('shared', { count: 2 });
    assert.deepStrictEqual(db1.read('shared'), { count: 2 }, 'Latest write should be visible to all instances');

    const db3 = new JSONBLite(DB_FILE);
    assert.deepStrictEqual(db3.read('shared'), { count: 2 }, 'Data should persist across re-open');
}

async function testDumpAndVacuum() {
    const db = new JSONBLite(DB_FILE);

    for (let i = 0; i < 100; i++) {
        db.write(`vacuum-${i}`, { i });
    }
    for (let i = 0; i < 100; i += 2) {
        db.delete(`vacuum-${i}`);
    }

    const streamPayload = await streamToString(db.dump());
    const streamDump = JSON.parse(streamPayload);
    assert.strictEqual(streamDump.meta.version, 1);
    assert.strictEqual(typeof streamDump.meta.data_size, 'number');
    assert.strictEqual(streamDump.data.string, 'updated');
    assert.strictEqual(streamDump.data['vacuum-1'].i, 1);
    assert.strictEqual(streamDump.data['vacuum-2'], undefined, 'Deleted keys should not be present in dump data');

    db.dump(DUMP_FILE);
    assert.strictEqual(fs.existsSync(DUMP_FILE), true, 'Dump filename should be written');
    const fileDump = JSON.parse(fs.readFileSync(DUMP_FILE, 'utf8'));
    assert.deepStrictEqual(fileDump.data.shared, { count: 2 }, 'File dump should include latest values');

    const sizeBeforeVacuum = fs.statSync(DB_FILE).size;
    db.vacuum();
    const sizeAfterVacuum = fs.statSync(DB_FILE).size;
    assert.strictEqual(sizeAfterVacuum < sizeBeforeVacuum, true, 'Vacuum should compact file size after deletions');
    assert.strictEqual(db.read('vacuum-2'), null, 'Deleted key should remain deleted after vacuum');
    assert.deepStrictEqual(db.read('vacuum-3'), { i: 3 }, 'Existing key should survive vacuum');
}

async function test() {
    cleanupFiles();

    console.log('Testing Basic Operations...');
    testBasicOperations();

    console.log('Testing Multiple Instances and Persistence...');
    testMultipleInstancesAndPersistence();

    console.log('Testing Dump and Vacuum...');
    await testDumpAndVacuum();
}

test()
    .then(() => {
        console.log('✅ jsonblite integration tests passed');
    })
    .catch((err) => {
        console.trace(err);
        console.error('❌ jsonblite integration tests failed:', err.message);
        process.exitCode = 1;
    })
    .finally(() => {
        cleanupFiles();
    });
