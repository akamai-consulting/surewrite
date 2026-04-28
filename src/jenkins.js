/**
 * Jenkins One-at-a-Time Hash (32-bit)
 * Non-cryptographic hash optimized for speed and uniform distribution.
 */
export function jenkinsHash(key) {
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash += key.charCodeAt(i);
    hash += (hash << 10);
    hash ^= (hash >>> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >>> 11);
  hash += (hash << 15);
  // Force to unsigned 32-bit integer
  return hash >>> 0;
}
