import JSONBLite from '../src/jsonblite.js';

const path = './example.jsonblite';

// Create new db.
const db = new JSONBLite(path, { verbose: true });

// Write all possible CBOR / JSON data types.
db.write('key1', 'string');
db.write('key2', 123);
db.write('key3', { key: 'value' });
db.write('key4', [1, 2, 3]);
db.write('key5', true);
db.write('key6', null);
db.write('key7', undefined);
db.write('key8', new Date());
db.write('key9', Infinity);
db.write('key10', NaN);

const db2 = new JSONBLite(path, { verbose: true });

// Get all keys.
const keys = db2.keys();
console.log('db2 keys: ', db2.keys());

// Get all values.
for (const key of keys) {
    console.log(db2.read(key));
}

// Delete some data.
db2.delete('key3');
db2.delete('key4');
db2.delete('key10');

// Vacuum the db.
db2.vacuum();

// Dump db.
db2.dump('./example.json');