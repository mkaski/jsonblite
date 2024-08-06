# JSONBLite: Single-file binary JSON database

JSONBLite is a single file, key-value binary JSON database, implemented as a TypeScript class for Node.js. A naive solution for persistent JSON storage, embeddable in Node.js applications. Operations are synchronous and it tries to be ACID-compliant, using file locks and journaling.

JSONBLite uses [CBOR](https://cbor.io) (Concise Binary Object Representation) standard to store binary JSON data. It's more compact and faster to parse than JSON. Any JSON data can be encoded/decoded in the database.

The index is a serialized [JavaScript `Map`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map#description), not a tree on disk.  The index is initialized in memory in its entirety as a `Map` to allow for fast lookups in memory.

See [jsonblite-example](https://github.com/mkaski/jsonblite-example) for LIVE DEMO of a simple server application running JSONBLite.

> **Warning**: Not recommended for any use in its current state. Expect data loss and corruption.

# Features

- Single file database
- Single TypeScript class in less than 500 lines of code
- CBOR for binary JSON storage
- ACID based on file locks and journaling
- In-memory `Map` index
- Append-only log, with a manual `vacuum()` garbage collection

#### Dependencies

- [cbor-x](https://github.com/kriszyp/cbor-x) for CBOR encoding/decoding
- [fs-ext](https://www.npmjs.com/package/fs-ext) for flock implementation

# Usage

Install the package from npm

```sh
npm i jsonblite
```

Use the class in your Node.js application

```typescript
import JSONBLite from 'jsonblite';

// initialize JSONBlite instance by reading or creating a database file
const db = new JSONBLite('./db.jsonblite');

db.write('k', { value: 'Hello, world!', number: 1 });
db.write('k2', 123);
db.read('k');
// -> { value: 'Hello, world!', number: 1 }
db.dump();
// -> { "meta": { "version": 1, "index_size": 10, "last_vacuum": 0 }, "data": { "k": { "value": "Hello, world!", "number": 1 }, "k2": 123 } }
db.delete('k2');
db.read('k');
// -> null
db.keys();
// -> [ 'k' ]
```

# API

## Constructor

- `new JSONBLite(filename: string, options?: { verbose: boolean })`: Create a new database instance and file.

## Methods

- `read(key: string)`: Read a value from the database.
- `write(key: string, value: any)`: Write a value to the database.
- `delete(key: string)`: Delete a value from the database index.
- `keys()`: Read all keys from the database.

## Maintenance

- `vacuum()`: Run to permanently remove deleted data from the database file, and compact the file.
- `dump(filename?: string)`: Dump the database to a JSON file.

# File Format

```plaintext
+-----------------+
| Header (fixed)  | 6A73 6F6E 626C 6974 6501 1C00 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 00a3
+-----------------+
| Data   (varlen) | 8261 6B76 616C 7565 6A48 656C 6C6F 2C20 776F 726C 6421 6963 6E75 6D31 6A62 6F6F 6C74 ...
+-----------------+
| Index  (varlen) | 646b 6579 3182 185c 0d64 6b65 7932 8218 5c0d 646b 6579 3382 185c 0d64 6b65 7934 8218 ...
+-----------------+
```

## Header

Header is a fixed 36-byte structure at the beginning of the file.

| Field           | Size      | Description                                      |
|-----------------|-----------|--------------------------------------------------|
| `magic`         | 9 bytes   | Magic number (`0x6A736F6E626C6974`, "jsonblite") |
| `version`       | 1 byte    | Version number (`0x01`)                          |
| `index_size`    | 4 bytes   | Index size (uint)                                |
| `data_size`     | 6 bytes   | Data size                                        |
| `last_modified` | 8 bytes   | Unix timestamp of last modification              |
| `last_vacuum`   | 8 bytes   | Unix timestamp of last vacuum                    |

## Index

In-memory, index is a variable length JavaScript `Map` of keys to record data `[offset, size]`. On disk, it's a CBOR-encoded Map.

| Field   | Type     | Description                               |
|---------|----------|-------------------------------------------|
| `key`   | `string` | any string                                |
| `offset`| `uint`   | Location of the data record offset        |
| `size`  | `uint`   | Size of the data record in bytes          |

## Data

Data is saved as a log of [CBOR](https://cbor.io)-encoded JSON records. Data is accessed by the offsets in the index.

# Dump

Dumped JSONBLite database is a JSON object with `meta` and `data` fields.

```json
{
    "meta": {
        "version": 1,
        "data_size": 48,
        "index_size": 28,
        "last_vacuum": 0
    },
    "data": { 
        "key": { "value": "Hello, world!", "num": 1, "bool": true },
        "key2": { "value": "Example", "bool": false }
     },
}
```