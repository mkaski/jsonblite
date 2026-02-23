import { encode } from "cbor-x";

// Index
export const INDEX_DEFAULT_CONTENT = encode(new Map());

// Header
export const HEADER_MAGIC = Buffer.from([0x6A, 0x73, 0x6F, 0x6E, 0x62, 0x6C, 0x69, 0x74, 0x65]); // jsonblite, 9 bytes
export const HEADER_VERSION = Buffer.from([1]); // 1, 1 byte
export const HEADER_INDEX_SIZE_BYTES = 4; // 4 bytes
export const HEADER_DATA_SIZE_BYTES = 6; // 6 bytes
export const HEADER_DEFAULT_INDEX_SIZE = Buffer.alloc(HEADER_INDEX_SIZE_BYTES); // 4 bytes
HEADER_DEFAULT_INDEX_SIZE.writeUInt32LE(INDEX_DEFAULT_CONTENT.length, 0);
export const HEADER_DEFAULT_DATA_SIZE = Buffer.alloc(HEADER_DATA_SIZE_BYTES); // 6 bytes
export const HEADER_LAST_MODIFIED_SIZE_BYTES = 8; // 8 bytes
export const HEADER_DEFAULT_LAST_MODIFIED = Buffer.alloc(HEADER_LAST_MODIFIED_SIZE_BYTES); // 8 bytes
export const HEADER_DEFAULT_LAST_VACUUM_TIMESTAMP = Buffer.alloc(8); // 8 bytes

// 9 + 1 + 4 + 6 + 8 + 8 = 36 bytes
export const HEADER_SIZE = HEADER_MAGIC.length
    + HEADER_VERSION.length
    + HEADER_DEFAULT_INDEX_SIZE.length
    + HEADER_DEFAULT_DATA_SIZE.length
    + HEADER_DEFAULT_LAST_MODIFIED.length
    + HEADER_DEFAULT_LAST_VACUUM_TIMESTAMP.length;

// Locations
export const LOCATION_MAGIC = 0;
export const LOCATION_VERSION = 9;
export const LOCATION_INDEX_SIZE = 10;
export const LOCATION_DATA_SIZE = 14;
export const LOCATION_LAST_MODIFIED = 20;
export const LOCATION_LAST_VACUUM_TIMESTAMP = 28;
export const LOCATION_DATA = HEADER_SIZE;

// Default values
const DEFAULT_HEADER_BYTES = Buffer.from([
    ...HEADER_MAGIC,
    ...HEADER_VERSION,
    ...HEADER_DEFAULT_INDEX_SIZE,
    ...HEADER_DEFAULT_DATA_SIZE,
    ...HEADER_DEFAULT_LAST_MODIFIED,
    ...HEADER_DEFAULT_LAST_VACUUM_TIMESTAMP
]);
export const DEFAULT_HEADER = Buffer.from(DEFAULT_HEADER_BYTES);
export const createDefaultHeader = () => Buffer.from(DEFAULT_HEADER_BYTES);
export const DATA_DEFAULT_CONTENT = Buffer.alloc(0);
export const DEFAULT_FILE_CONTENT = Buffer.from([...DEFAULT_HEADER_BYTES, ...DATA_DEFAULT_CONTENT, ...INDEX_DEFAULT_CONTENT]);

export const DEFAULT_OPTIONS = {
    verbose: false,
};

// Messages
export const LOG_PREFIX = '[JSONBLite]';
export const LOG_ERROR_PREFIX = '[ERROR]';
export const LOG_INFO_PREFIX = '[INFO]';

export const ERROR_INVALID_KEY = 'Invalid key. Key must be a non-empty string.';
