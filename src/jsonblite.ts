import fs from 'fs';
import { flockSync } from 'fs-ext';
import { encode, decode } from 'cbor-x';
import { PassThrough } from 'stream';

import {
    DEFAULT_FILE_CONTENT,
    DEFAULT_HEADER,
    DEFAULT_OPTIONS,
    ERROR_INVALID_KEY,
    HEADER_LAST_MODIFIED_SIZE_BYTES,
    HEADER_SIZE,
    LOCATION_DATA_SIZE,
    LOCATION_INDEX_SIZE,
    LOCATION_LAST_MODIFIED,
    LOCATION_LAST_VACUUM_TIMESTAMP,
    LOCATION_VERSION,
    LOG_ERROR_PREFIX,
    LOG_INFO_PREFIX,
    LOG_PREFIX,
} from './constants.js';

interface Options {
    verbose?: boolean;
}

interface Transaction {
    key: string;
    operation: string;
    data: any;
    index: Buffer;
    header: Buffer;
    dataOffset: number;
}

export default class JSONBLite {
    filename: string;
    header: Buffer; // Header bytes
    index: Map<string, [number, number]>; // Key: [offset, size]
    dataTail: number; // Offset to append new data
    options: Options;
    lastModified: BigInt;

    constructor(filename: string, options?: Options) {
        this.filename = filename;
        this.header = DEFAULT_HEADER;
        this.index = new Map();
        this.dataTail = HEADER_SIZE + this.header.readUInt32LE(LOCATION_DATA_SIZE);
        this.options = { ...DEFAULT_OPTIONS, ...options };
        this.lastModified = this.header.readBigInt64LE(LOCATION_LAST_MODIFIED);

        if (fs.existsSync(filename)) {
            // Recover if a journal file exists. Before recovery, the file might be corrupted.
            const journalExists = fs.existsSync(`${filename}.journal`);
            if (journalExists) {
                this.recover();
            }
            // Read the header and index from file, using a shared lock.
            const fd = fs.openSync(filename, 'r');
            this.sharedLock(fd);
            try {
                // Build the header and index from file.
                this.buildHeaderAndIndex(fd);
            } finally {
                this.unlock(fd);
                fs.closeSync(fd);
            }
        } else {
            this.initializeFile();
        }
    }

    private initializeFile() {
        // Write the default file content.
        const fd = fs.openSync(this.filename, 'w+');
        this.lock(fd);
        try {
            fs.writeSync(fd, DEFAULT_FILE_CONTENT, 0, DEFAULT_FILE_CONTENT.length, 0);
            this.buildHeaderAndIndex(fd);
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    private buildHeaderAndIndex(fd: number) {
        // Read header from file.
        fs.readSync(fd, this.header, 0, HEADER_SIZE, 0);
        // Read index and data size from header.
        const dataSize = this.header.readUInt32LE(LOCATION_DATA_SIZE);
        const indexSize = this.header.readUInt32LE(LOCATION_INDEX_SIZE);
        const indexLocation = HEADER_SIZE + dataSize;
        // Read index from file.
        const indexBytes = Buffer.alloc(indexSize);
        fs.readSync(fd, indexBytes, 0, indexSize, indexLocation);
        // const indexMap = decode(indexBytes); // Decode CBOR directly to Map.
        // Update in-memory state.
        this.index = decode(indexBytes); // Decode CBOR directly to Map.
        this.dataTail = indexLocation;
        this.lastModified = this.header.readBigInt64LE(LOCATION_LAST_MODIFIED);
    }

    private validateKey(key: string) {
        if (typeof key !== 'string' || key.length === 0) {
            return false;
        }
        return true;
    }

    private lock(fd: number) {
        try {
            flockSync(fd, 'ex');
        } catch (err: any) {
            throw new Error(`Failed to acquire lock: ${err.message}`);
        }
    }

    private unlock(fd: number) {
        try {
            flockSync(fd, 'un');
        } catch (err: any) {
            throw new Error(`Failed to release lock: ${err.message}`);
        }
    }

    private sharedLock(fd: number) {
        try {
            flockSync(fd, 'sh');
        } catch (err: any) {
            throw new Error(`Failed to acquire shared lock: ${err.message}`);
        }
    }

    private sync(fd: number) {
        // Check if journal file exists and recover if so.
        const journalExists = fs.existsSync(`${this.filename}.journal`);
        if (journalExists) {
            this.recover();
        }
        // Check if last modified time has changed since last read.
        // Rebuild header from the file if we recovered from journal OR last modified time has changed.
        const lastModifiedOnFile = Buffer.alloc(HEADER_LAST_MODIFIED_SIZE_BYTES);
        fs.readSync(fd, lastModifiedOnFile, 0, HEADER_LAST_MODIFIED_SIZE_BYTES, LOCATION_LAST_MODIFIED);
        if (lastModifiedOnFile.readBigInt64LE() !== this.lastModified) {
            this.buildHeaderAndIndex(fd);
        }
    }

    private beginTransaction(transaction: Transaction) {
        this.log(`${LOG_INFO_PREFIX} BEGIN TRANSACTION `, transaction);
        // Write journal on every latest transaction.
        const serialized = encode(transaction);
        fs.writeFileSync(`${this.filename}.journal`, serialized);
    }

    private performTransaction(transaction: Transaction, fd: number) {
        switch (transaction.operation) {
            case 'write':
                // Append new data to file.
                fs.writeSync(fd, transaction.data, 0, transaction.data.length, transaction.dataOffset - transaction.data.length);
                break;
            case 'delete':
                // Updating the index and header is sufficient for a delete operation.
                break;
            default:
                throw new Error(`${LOG_PREFIX} Invalid operation`);
        }

        // Common operations for all transactions.
        // Write header to file
        fs.writeSync(fd, transaction.header, 0, HEADER_SIZE, 0);
        // this.log(`${LOG_INFO_PREFIX} TX: Wrote header`);
        // Append new index to end of file.
        const indexLocation = transaction.dataOffset;
        fs.writeSync(fd, transaction.index, 0, transaction.index.length, indexLocation);
        // this.log(`${LOG_INFO_PREFIX} TX: Wrote index`);
    }

    private commitTransaction() {
        // Delete journal file on successful write.
        fs.unlinkSync(`${this.filename}.journal`);
    }

    private recover() {
        this.log(`${LOG_INFO_PREFIX} RECOVERING FROM JOURNAL`);
        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);
        try {
            // Recover from a crash by replaying the journal transaction.
            const journalBuffer = fs.readFileSync(`${this.filename}.journal`);
            const transaction: Transaction = decode(journalBuffer);
            this.performTransaction(transaction, fd);
            this.commitTransaction();
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    private log(message: string, payload?: any) {
        if (this.options.verbose) {
            console.log(`${LOG_PREFIX} ${message}`, payload ?? '');
        }
    }

    read(key: string) {
        if (!this.validateKey(key)) {
            throw new Error(`${LOG_PREFIX} ${ERROR_INVALID_KEY}`);
        }

        const fd = fs.openSync(this.filename, 'r');
        this.sharedLock(fd);

        try {
            this.sync(fd);
            // Search for the key in the index.
            const [offset, size] = this.index.get(key) ?? [null, null];
            if (offset === null) {
                return null;
            }
            // Read data from file.
            const readBuffer = Buffer.alloc(size);
            fs.readSync(fd, readBuffer, 0, size, offset);
            return decode(readBuffer);
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    write(key: string, value: any) {
        if (!this.validateKey(key)) {
            throw new Error(`${LOG_PREFIX} ${ERROR_INVALID_KEY}`);
        }

        // Lock the file.
        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);

        try {
            this.sync(fd);
            // Serialize value as CBOR.
            const serialized = encode(value);
            // Update in-memory.
            this.index.set(key, [this.dataTail, serialized.length]);
            const indexBytes = encode(this.index);
            const indexSize = indexBytes.length;
            const lastModified = BigInt(Date.now());
            this.lastModified = lastModified;
            this.dataTail += serialized.length;
            this.header.writeBigInt64LE(lastModified, LOCATION_LAST_MODIFIED);
            this.header.writeUInt32LE(indexSize, LOCATION_INDEX_SIZE);
            this.header.writeUInt32LE(this.dataTail - HEADER_SIZE, LOCATION_DATA_SIZE);
            // Build the uniform transaction payload to write to file.
            const transaction: Transaction = {
                key,
                operation: 'write',
                header: this.header,
                data: serialized,
                index: indexBytes,
                dataOffset: this.dataTail
            }
            // Begin, perform, and commit the transaction.
            this.beginTransaction(transaction);
            this.performTransaction(transaction, fd);
            this.commitTransaction();
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    delete(key: string) {
        if (!this.validateKey(key)) {
            throw new Error(`${LOG_PREFIX}${LOG_ERROR_PREFIX}${ERROR_INVALID_KEY}`);
        }

        // Lock the file.
        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);

        try {
            // Sync the file to ensure the latest data.
            this.sync(fd);
            // Delete key from index.
            this.index.delete(key);
            // Update index in header.
            const indexBytes = encode(this.index);
            const indexSize = indexBytes.length;
            const lastModified = BigInt(Date.now());
            this.lastModified = lastModified;
            this.header.writeUInt32LE(indexSize, LOCATION_INDEX_SIZE);
            this.header.writeBigInt64LE(lastModified, LOCATION_LAST_MODIFIED);
            // Build the uniform transaction payload to write to file.
            const transaction: Transaction = {
                key,
                operation: 'delete',
                header: this.header,
                data: null,
                index: indexBytes,
                dataOffset: this.dataTail
            }
            // Begin, perform, and commit the transaction.
            this.beginTransaction(transaction);
            this.performTransaction(transaction, fd);
            this.commitTransaction();
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    dump(): PassThrough;
    dump(filename: string): void;
    dump(filename?: string): PassThrough | void {
        const meta = {
            version: this.header.readUInt8(LOCATION_VERSION),
            data_size: this.header.readUInt32LE(LOCATION_DATA_SIZE),
            index_size: this.header.readUInt32LE(LOCATION_INDEX_SIZE),
            last_vacuum: this.header.readBigInt64LE(LOCATION_LAST_VACUUM_TIMESTAMP).toString(), // BigInt to string. JSON: "Do not know how to serialize a BigInt".
        };
        // Stream JSON dump to a file or return a PassThrough stream to caller.
        const stream = filename ? fs.createWriteStream(filename) : new PassThrough();
        stream.write('{\n');
        stream.write(`  "meta": ${JSON.stringify(meta, null, 2)},\n`);
        stream.write('  "data": {\n');
        const entries = Array.from(this.index.entries());
        entries.forEach(([key, [offset, size]], index) => {
            const buffer = Buffer.alloc(size);
            const fd = fs.openSync(this.filename, 'r');
            fs.readSync(fd, buffer, 0, size, offset);
            fs.closeSync(fd);
            const value = decode(buffer);
            const keyValueString = `    "${key}": ${JSON.stringify(value, null, 2)}`;
            if (index < entries.length - 1) {
                stream.write(`${keyValueString},\n`);
            } else {
                stream.write(`${keyValueString}\n`);
            }
        });
        stream.write('  }\n');
        stream.write('}\n');
        stream.end();

        if (!filename) {
            return stream as PassThrough;
        }
    }


    keys() {
        const fd = fs.openSync(this.filename, 'r');
        this.sharedLock(fd);
        try {
            this.sync(fd);
            // Return the keys from the index.
            return Array.from(this.index.keys());
        } finally {
            // Ensure the file is unlocked and closed even if an error occurs.
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    vacuum() {
        // Temporary file for the new vacuumed database
        const tempFilename = `${this.filename}.temp`;
        const tempFd = fs.openSync(tempFilename, 'w');

        // Acquire exclusive lock on the db
        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);

        try {
            this.sync(fd);
            const newHeader = DEFAULT_HEADER;
            fs.writeSync(tempFd, newHeader, 0, HEADER_SIZE, 0);

            const newIndex = new Map<string, [number, number]>();
            let newDataOffset = HEADER_SIZE;
            for (const [key, [offset, size]] of this.index) {
                // Read the data from the old file. TODO: Maybe stream this?
                const dataBuffer = Buffer.alloc(size);
                fs.readSync(fd, dataBuffer, 0, size, offset);
                fs.writeSync(tempFd, dataBuffer, 0, size, newDataOffset);
                // Update the new index with the new offset
                newIndex.set(key, [newDataOffset, size]);
                // Update the new data offset
                newDataOffset += size;
            }
            const indexBytes = encode(newIndex);
            const indexSize = indexBytes.length;

            // Write the updated header and index to the temporary file
            newHeader.writeUInt32LE(indexSize, LOCATION_INDEX_SIZE);
            newHeader.writeUInt32LE(newDataOffset - HEADER_SIZE, LOCATION_DATA_SIZE);
            newHeader.writeBigInt64LE(BigInt(Date.now()), LOCATION_LAST_VACUUM_TIMESTAMP);
            fs.writeSync(tempFd, newHeader, 0, HEADER_SIZE, 0);
            fs.writeSync(tempFd, indexBytes, 0, indexSize, newDataOffset);
            fs.closeSync(tempFd);

            // Log size difference
            const oldSize = fs.statSync(this.filename).size;
            const newSize = fs.statSync(tempFilename).size;
            this.log(`${LOG_INFO_PREFIX} Vacuumed database: ${oldSize} bytes -> ${newSize} bytes`);

            // Replace the original file with the compacted temporary file
            fs.renameSync(tempFilename, this.filename);

            // Update in-memory
            this.header = newHeader;
            this.index = newIndex;
            this.dataTail = newDataOffset;

        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }
}