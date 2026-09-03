const MASK_64 = (1n << 64n) - 1n;
const C1 = 0x87c37b91114253d5n;
const C2 = 0x4cf5ad432745937fn;

const rotateLeft = (value, amount) =>
  ((value << BigInt(amount)) | (value >> BigInt(64 - amount))) & MASK_64;

const mixFinal = (input) => {
  let value = input;
  value ^= value >> 33n;
  value = (value * 0xff51afd7ed558ccdn) & MASK_64;
  value ^= value >> 33n;
  value = (value * 0xc4ceb9fe1a85ec53n) & MASK_64;
  return value ^ (value >> 33n);
};

const readLittleEndian = (bytes, start, length) => {
  let value = 0n;
  for (let index = length - 1; index >= 0; index -= 1) {
    value = (value << 8n) | BigInt(bytes[start + index]);
  }
  return value;
};

export const murmur3X64_128 = (input) => {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  let h1 = 0n;
  let h2 = 0n;
  const complete = bytes.length - (bytes.length % 16);

  for (let offset = 0; offset < complete; offset += 16) {
    let k1 = readLittleEndian(bytes, offset, 8);
    let k2 = readLittleEndian(bytes, offset + 8, 8);
    k1 = (rotateLeft((k1 * C1) & MASK_64, 31) * C2) & MASK_64;
    h1 ^= k1;
    h1 = (((rotateLeft(h1, 27) + h2) & MASK_64) * 5n + 0x52dce729n) & MASK_64;
    k2 = (rotateLeft((k2 * C2) & MASK_64, 33) * C1) & MASK_64;
    h2 ^= k2;
    h2 = (((rotateLeft(h2, 31) + h1) & MASK_64) * 5n + 0x38495ab5n) & MASK_64;
  }

  const tailLength = bytes.length - complete;
  const k1Length = Math.min(tailLength, 8);
  const k2Length = Math.max(tailLength - 8, 0);
  const k1 = readLittleEndian(bytes, complete, k1Length);
  const k2 = readLittleEndian(bytes, complete + 8, k2Length);
  if (k2 !== 0n) {
    h2 ^= (rotateLeft((k2 * C2) & MASK_64, 33) * C1) & MASK_64;
  }
  if (k1 !== 0n) {
    h1 ^= (rotateLeft((k1 * C1) & MASK_64, 31) * C2) & MASK_64;
  }

  const length = BigInt(bytes.length);
  h1 ^= length;
  h2 ^= length;
  h1 = (h1 + h2) & MASK_64;
  h2 = (h2 + h1) & MASK_64;
  h1 = mixFinal(h1);
  h2 = mixFinal(h2);
  h1 = (h1 + h2) & MASK_64;
  h2 = (h2 + h1) & MASK_64;

  return `${h2.toString(16).padStart(16, "0")}${h1.toString(16).padStart(16, "0")}`;
};
