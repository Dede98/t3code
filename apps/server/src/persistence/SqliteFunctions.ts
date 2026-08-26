export const SQLITE_FATAL_UTF8_FUNCTION = "t3_fatal_utf8";

const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const utf8Encoder = new TextEncoder();

/** Pure implementation shared by every registration of the durable SQLite UTF-8 guard. */
export const isFatalUtf8Blob = (value: unknown): 0 | 1 => {
  if (!(value instanceof Uint8Array)) return 0;

  try {
    const decoded = fatalUtf8Decoder.decode(value);
    const roundTrip = utf8Encoder.encode(decoded);
    if (roundTrip.byteLength !== value.byteLength) return 0;
    for (let index = 0; index < value.byteLength; index += 1) {
      if (roundTrip[index] !== value[index]) return 0;
    }
    return 1;
  } catch {
    return 0;
  }
};
