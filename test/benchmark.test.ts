import JSONBLite from '../src/jsonblite.js';
import { promises as fs } from 'fs';

const NUM_OPERATIONS = 10_000;
const path = './benchmark.jsonblite';

const runBenchmark = () => {
    const db = new JSONBLite(path, { verbose: false });

    console.log(`Running benchmark with ${NUM_OPERATIONS} operations...`);
    console.time('Total write time');
    for (let i = 0; i < NUM_OPERATIONS; i++) {
        db.write(`key${i}`, { value: `value${i}`, number: i });
    }
    console.timeEnd('Total write time');

    console.time('Total read time');
    for (let i = 0; i < NUM_OPERATIONS; i++) {
        db.read(`key${i}`);
    }
    console.timeEnd('Total read time');

    console.time('Total delete time');
    for (let i = 0; i < NUM_OPERATIONS; i++) {
        db.delete(`key${i}`);
    }
    console.timeEnd('Total delete time');

    // Cleanup
    fs.unlink(path).catch(err => console.error('Failed to delete the file:', err));
};

runBenchmark();
