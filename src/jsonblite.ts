import fs from 'fs';
import { flockSync } from 'fs-ext';
import { encode, decode } from 'cbor-x';
import { PassThrough } from 'stream';

import {
    DEFAULT_FILE_CONTENT,
    DEFAULT_OPTIONS,
    ERROR_INVALID_KEY,
    HEADER_DATA_SIZE_BYTES,
    HEADER_SIZE,
    LOCATION_DATA_SIZE,
    LOCATION_INDEX_SIZE,
    LOCATION_LAST_MODIFIED,
    LOCATION_LAST_VACUUM_TIMESTAMP,
    LOCATION_VERSION,
    LOG_ERROR_PREFIX,
    LOG_INFO_PREFIX,
    LOG_PREFIX,
    createDefaultHeader,
} from './constants.ts';

interface Options {
    verbose?: boolean;
}

interface Transaction {
    key: string;
    operation: 'write' | 'delete';
    data: Buffer | null;
    index: Buffer;
    header: Buffer;
    dataOffset: number;
}

type IndexMap = Map<string, [number, number]>;

export default class JSONBLite {
    filename: string;
    header: Buffer; // Header bytes
    index: IndexMap; // Key: [offset, size]
    dataTail: number; // Offset to append new data
    lastModified: bigint;
    options: Options;

    constructor(filename: string, options?: Options) {
        this.filename = filename;
        this.header = createDefaultHeader();
        this.index = new Map();
        this.dataTail = HEADER_SIZE + this.readDataSizeFromHeader();
        this.lastModified = this.header.readBigInt64LE(LOCATION_LAST_MODIFIED);
        this.options = { ...DEFAULT_OPTIONS, ...options };

        if (fs.existsSync(filename)) {
            const fd = fs.openSync(filename, 'r+');
            this.lock(fd);
            try {
                this.sync(fd, true);
            } finally {
                this.unlock(fd);
                fs.closeSync(fd);
            }
        } else {
            this.initializeFile();
        }
    }

    private readDataSizeFromHeader() {
        return this.header.readUIntLE(LOCATION_DATA_SIZE, HEADER_DATA_SIZE_BYTES);
    }

    private writeDataSizeToHeader(dataSize: number) {
        this.header.writeUIntLE(dataSize, LOCATION_DATA_SIZE, HEADER_DATA_SIZE_BYTES);
    }

    private initializeFile() {
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
        const nextHeader = Buffer.alloc(HEADER_SIZE);
        fs.readSync(fd, nextHeader, 0, HEADER_SIZE, 0);

        const dataSize = nextHeader.readUIntLE(LOCATION_DATA_SIZE, HEADER_DATA_SIZE_BYTES);
        const indexSize = nextHeader.readUInt32LE(LOCATION_INDEX_SIZE);
        const indexLocation = HEADER_SIZE + dataSize;
        const fileSize = fs.fstatSync(fd).size;
        if (indexLocation + indexSize > fileSize) {
            throw new Error(`${LOG_PREFIX} File is truncated or corrupted`);
        }

        const indexBytes = Buffer.alloc(indexSize);
        fs.readSync(fd, indexBytes, 0, indexSize, indexLocation);
        const decodedIndex = decode(indexBytes);
        if (!(decodedIndex instanceof Map)) {
            throw new Error(`${LOG_PREFIX} Invalid index format`);
        }

        this.header = nextHeader;
        this.index = decodedIndex as IndexMap;
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

    private recoverWithFileDescriptor(fd: number, journalPath: string) {
        this.log(`${LOG_INFO_PREFIX} RECOVERING FROM JOURNAL`);
        const journalBuffer = fs.readFileSync(journalPath);
        const transaction: Transaction = decode(journalBuffer);
        this.performTransaction(transaction, fd);
        this.commitTransaction();
    }

    private recover() {
        const journalPath = `${this.filename}.journal`;
        if (!fs.existsSync(journalPath)) {
            return;
        }
        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);
        try {
            this.recoverWithFileDescriptor(fd, journalPath);
            this.buildHeaderAndIndex(fd);
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    private nextLastModified() {
        const now = BigInt(Date.now());
        return now > this.lastModified ? now : this.lastModified + 1n;
    }

    private sync(fd: number, recoverIfJournal = false) {
        const journalPath = `${this.filename}.journal`;
        if (recoverIfJournal && fs.existsSync(journalPath)) {
            this.recoverWithFileDescriptor(fd, journalPath);
        }
        const lastModifiedOnFile = Buffer.alloc(8);
        fs.readSync(fd, lastModifiedOnFile, 0, 8, LOCATION_LAST_MODIFIED);
        const fileLastModified = lastModifiedOnFile.readBigInt64LE();
        if (fileLastModified !== this.lastModified) {
            this.buildHeaderAndIndex(fd);
        }
    }

    private beginTransaction(transaction: Transaction) {
        this.log(`${LOG_INFO_PREFIX} BEGIN TRANSACTION`, transaction);
        const serialized = encode(transaction);
        fs.writeFileSync(`${this.filename}.journal`, serialized);
    }

    private performTransaction(transaction: Transaction, fd: number) {
        switch (transaction.operation) {
            case 'write':
                if (!transaction.data) {
                    throw new Error(`${LOG_PREFIX} Missing transaction data for write`);
                }
                fs.writeSync(fd, transaction.data, 0, transaction.data.length, transaction.dataOffset - transaction.data.length);
                break;
            case 'delete':
                break;
            default:
                throw new Error(`${LOG_PREFIX} Invalid operation`);
        }

        fs.writeSync(fd, transaction.header, 0, HEADER_SIZE, 0);
        const indexLocation = transaction.dataOffset;
        fs.writeSync(fd, transaction.index, 0, transaction.index.length, indexLocation);
    }

    private commitTransaction() {
        const journalPath = `${this.filename}.journal`;
        if (fs.existsSync(journalPath)) {
            fs.unlinkSync(journalPath);
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

        this.recover();
        const fd = fs.openSync(this.filename, 'r');
        this.sharedLock(fd);
        try {
            this.sync(fd);
            const [offset, size] = this.index.get(key) ?? [null, null];
            if (offset === null || size === null) {
                return null;
            }
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

        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);
        try {
            this.sync(fd, true);
            const serialized = encode(value);
            this.index.set(key, [this.dataTail, serialized.length]);
            const indexBytes = encode(this.index);
            const indexSize = indexBytes.length;
            const lastModified = this.nextLastModified();
            this.dataTail += serialized.length;
            this.lastModified = lastModified;
            this.header.writeBigInt64LE(lastModified, LOCATION_LAST_MODIFIED);
            this.header.writeUInt32LE(indexSize, LOCATION_INDEX_SIZE);
            this.writeDataSizeToHeader(this.dataTail - HEADER_SIZE);

            const transaction: Transaction = {
                key,
                operation: 'write',
                header: Buffer.from(this.header),
                data: Buffer.from(serialized),
                index: Buffer.from(indexBytes),
                dataOffset: this.dataTail,
            };
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

        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);
        try {
            this.sync(fd, true);
            this.index.delete(key);
            const indexBytes = encode(this.index);
            const indexSize = indexBytes.length;
            const lastModified = this.nextLastModified();
            this.lastModified = lastModified;
            this.header.writeUInt32LE(indexSize, LOCATION_INDEX_SIZE);
            this.header.writeBigInt64LE(lastModified, LOCATION_LAST_MODIFIED);
            this.writeDataSizeToHeader(this.dataTail - HEADER_SIZE);

            const transaction: Transaction = {
                key,
                operation: 'delete',
                header: Buffer.from(this.header),
                data: null,
                index: Buffer.from(indexBytes),
                dataOffset: this.dataTail,
            };
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
        this.recover();
        const fd = fs.openSync(this.filename, 'r');
        this.sharedLock(fd);

        try {
            this.sync(fd);
            const meta = {
                version: this.header.readUInt8(LOCATION_VERSION),
                data_size: this.header.readUIntLE(LOCATION_DATA_SIZE, HEADER_DATA_SIZE_BYTES),
                index_size: this.header.readUInt32LE(LOCATION_INDEX_SIZE),
                last_vacuum: this.header.readBigInt64LE(LOCATION_LAST_VACUUM_TIMESTAMP).toString(),
            };
            const data: Record<string, unknown> = {};
            for (const [key, [offset, size]] of this.index.entries()) {
                const buffer = Buffer.alloc(size);
                fs.readSync(fd, buffer, 0, size, offset);
                data[key] = decode(buffer);
            }

            const payload = `${JSON.stringify({ meta, data }, null, 2)}\n`;
            if (filename) {
                fs.writeFileSync(filename, payload);
                return;
            }

            const stream = new PassThrough();
            stream.end(payload);
            return stream;
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    keys() {
        this.recover();
        const fd = fs.openSync(this.filename, 'r');
        this.sharedLock(fd);
        try {
            this.sync(fd);
            return Array.from(this.index.keys());
        } finally {
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }

    vacuum() {
        const tempFilename = `${this.filename}.temp`;
        let tempFd: number | null = null;

        const fd = fs.openSync(this.filename, 'r+');
        this.lock(fd);

        try {
            this.sync(fd, true);
            tempFd = fs.openSync(tempFilename, 'w');

            const newHeader = createDefaultHeader();
            fs.writeSync(tempFd, newHeader, 0, HEADER_SIZE, 0);

            const newIndex = new Map<string, [number, number]>();
            let newDataOffset = HEADER_SIZE;
            for (const [key, [offset, size]] of this.index.entries()) {
                const dataBuffer = Buffer.alloc(size);
                fs.readSync(fd, dataBuffer, 0, size, offset);
                fs.writeSync(tempFd, dataBuffer, 0, size, newDataOffset);
                newIndex.set(key, [newDataOffset, size]);
                newDataOffset += size;
            }

            const indexBytes = encode(newIndex);
            const indexSize = indexBytes.length;
            const now = this.nextLastModified();
            newHeader.writeUInt32LE(indexSize, LOCATION_INDEX_SIZE);
            newHeader.writeUIntLE(newDataOffset - HEADER_SIZE, LOCATION_DATA_SIZE, HEADER_DATA_SIZE_BYTES);
            newHeader.writeBigInt64LE(now, LOCATION_LAST_MODIFIED);
            newHeader.writeBigInt64LE(now, LOCATION_LAST_VACUUM_TIMESTAMP);

            fs.writeSync(tempFd, newHeader, 0, HEADER_SIZE, 0);
            fs.writeSync(tempFd, indexBytes, 0, indexSize, newDataOffset);
            fs.ftruncateSync(tempFd, newDataOffset + indexSize);
            fs.fsyncSync(tempFd);
            fs.closeSync(tempFd);
            tempFd = null;

            const oldSize = fs.fstatSync(fd).size;
            const newSize = fs.statSync(tempFilename).size;
            this.log(`${LOG_INFO_PREFIX} Vacuumed database: ${oldSize} bytes -> ${newSize} bytes`);

            fs.renameSync(tempFilename, this.filename);

            this.header = Buffer.from(newHeader);
            this.index = newIndex;
            this.dataTail = newDataOffset;
            this.lastModified = now;
        } finally {
            if (tempFd !== null) {
                fs.closeSync(tempFd);
            }
            if (fs.existsSync(tempFilename)) {
                fs.unlinkSync(tempFilename);
            }
            this.unlock(fd);
            fs.closeSync(fd);
        }
    }
}
