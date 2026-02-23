import fs from 'fs';
import assert from 'assert';
import { performance } from 'perf_hooks';
import JSONBLite from '../src/jsonblite.ts';

const NUM_OPERATIONS = 10_000;
const DB_FILE = './data/benchmark.jsonblite';
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

function fileSize(filename: string) {
    if (!fs.existsSync(filename)) {
        return 0;
    }
    return fs.statSync(filename).size;
}

function formatBytes(bytes: number) {
    const kib = bytes / 1024;
    const mib = kib / 1024;
    if (mib >= 1) {
        return `${mib.toFixed(2)} MiB`;
    }
    return `${kib.toFixed(2)} KiB`;
}

function formatOpsPerSec(opsPerSec: number) {
    return `${Math.round(opsPerSec).toLocaleString('en-US')} ops/s`;
}

function runPhase(name: string, operations: number, fn: () => void) {
    const start = performance.now();
    fn();
    const durationMs = performance.now() - start;
    const opsPerSec = operations === 0 ? 0 : operations / (durationMs / 1000);
    console.log(`${name.padEnd(22)} ${durationMs.toFixed(1).padStart(10)} ms   ${formatOpsPerSec(opsPerSec).padStart(14)}`);
    return { durationMs, opsPerSec };
}

function runBenchmark() {
    cleanupFiles();
    const db = new JSONBLite(DB_FILE, { verbose: false });

    console.log(`Running benchmark with ${NUM_OPERATIONS.toLocaleString('en-US')} records\n`);
    console.log('Phase'.padEnd(22), 'Duration'.padStart(10), 'Throughput'.padStart(18));
    console.log('-'.repeat(58));

    runPhase('insert records', NUM_OPERATIONS, () => {
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            db.write(`key${i}`, {
                number: i,
                label: `value-${i}`,
                parity: i % 2 === 0 ? 'even' : 'odd',
            });
        }
    });

    const sizeAfterInsert = fileSize(DB_FILE);
    const keysAfterInsert = db.keys().length;

    runPhase('overwrite records', NUM_OPERATIONS, () => {
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            db.write(`key${i}`, {
                number: i,
                label: `value-updated-${i}`,
                parity: i % 2 === 0 ? 'even' : 'odd',
            });
        }
    });

    const sizeAfterOverwrite = fileSize(DB_FILE);

    let checksum = 0;
    runPhase('read records', NUM_OPERATIONS, () => {
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            const value = db.read(`key${i}`) as { number: number } | null;
            if (value) {
                checksum += value.number;
            }
        }
    });

    runPhase('delete half records', NUM_OPERATIONS / 2, () => {
        for (let i = 0; i < NUM_OPERATIONS; i += 2) {
            db.delete(`key${i}`);
        }
    });

    const sizeAfterDelete = fileSize(DB_FILE);
    const keysAfterDelete = db.keys().length;

    runPhase('vacuum', 1, () => {
        db.vacuum();
    });

    const sizeAfterVacuum = fileSize(DB_FILE);
    const reopened = new JSONBLite(DB_FILE, { verbose: false });
    const keysAfterReopen = reopened.keys().length;

    assert.strictEqual(keysAfterInsert, NUM_OPERATIONS, 'Insert phase key count mismatch');
    assert.strictEqual(keysAfterDelete, NUM_OPERATIONS / 2, 'Delete phase key count mismatch');
    assert.strictEqual(keysAfterReopen, NUM_OPERATIONS / 2, 'Reopen key count mismatch');
    assert.strictEqual(reopened.read('key0'), null, 'Deleted key should be missing');
    assert.deepStrictEqual(reopened.read('key1'), {
        number: 1,
        label: 'value-updated-1',
        parity: 'odd',
    }, 'Existing key should be persisted');

    console.log('\nFile size stats');
    console.log(`After insert:      ${formatBytes(sizeAfterInsert)} (${sizeAfterInsert.toLocaleString('en-US')} bytes)`);
    console.log(`After overwrite:   ${formatBytes(sizeAfterOverwrite)} (${sizeAfterOverwrite.toLocaleString('en-US')} bytes)`);
    console.log(`After delete:      ${formatBytes(sizeAfterDelete)} (${sizeAfterDelete.toLocaleString('en-US')} bytes)`);
    console.log(`After vacuum:      ${formatBytes(sizeAfterVacuum)} (${sizeAfterVacuum.toLocaleString('en-US')} bytes)`);
    console.log(`Vacuum reclaimed:  ${(100 * (sizeAfterDelete - sizeAfterVacuum) / sizeAfterDelete).toFixed(2)}%`);

    console.log('\nSanity checks');
    console.log(`Read checksum:     ${checksum.toLocaleString('en-US')}`);
    console.log(`Keys after insert: ${keysAfterInsert.toLocaleString('en-US')}`);
    console.log(`Keys after delete: ${keysAfterDelete.toLocaleString('en-US')}`);
    console.log(`Keys after reopen: ${keysAfterReopen.toLocaleString('en-US')}`);
    console.log('\n✅ Benchmark completed');
}

try {
    runBenchmark();
} catch (err: any) {
    console.error('❌ Benchmark failed:', err.message);
    process.exitCode = 1;
} finally {
    cleanupFiles();
}
