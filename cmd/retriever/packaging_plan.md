# Retriever Packaging And Encryption Plan

## Goal

Add optional packaging and encryption for retriever dumps.

The package should preserve the existing manifest-based dump collection while
allowing operators to produce a single encrypted archive suitable for storage or
transfer.

Assumption: "QC resistant" means quantum-computer-resistant cryptography.

## Recommended Design

Use a raw TAR packaging layer plus an encrypted streaming envelope:

```text
retriever dump -> dump directory -> raw tar stream -> HPKE/ML-KEM encrypted archive
```

Default crypto suite:

```text
HPKE:
  KEM:  ML-KEM-1024
  KDF:  HKDF-SHA512
  AEAD: AES-256-GCM
```

NIST FIPS 203 standardizes ML-KEM for post-quantum key establishment. The
current Go toolchain exposes `crypto/mlkem` and `crypto/hpke`, so this should
not require a third-party post-quantum implementation.

## CLI Shape

Generate recipient keys:

```bash
retriever keygen \
  -private retriever-private.key \
  -public retriever-public.key
```

Create an encrypted archive during dump:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub full \
  -salt "$RETRIEVER_SCRUB_SALT" \
  -archive-out ./dump.tar.pq \
  -recipient ./retriever-public.key
```

Unpack an encrypted archive:

```bash
retriever unpack \
  -archive ./dump.tar.pq \
  -identity ./retriever-private.key \
  -out ./dumpdir
```

Optional later convenience:

```bash
retriever load \
  -archive ./dump.tar.pq \
  -identity ./retriever-private.key \
  -connection "$CONNECTION_STRING"
```

## Archive Format

Use uncompressed TAR only:

- No `.tar.gz`.
- No `.tgz`.
- No zstd archive compression.
- Archive packaging uses Go's `archive/tar`.

The existing fragment compression setting remains independent. If the full
plaintext dump also needs uncompressed fragments, add that separately as
`-compression none`.

TAR writer rules:

- Deterministic sorted paths.
- Normalized slash paths.
- No absolute paths.
- No `..` path traversal.
- No symlinks.
- Regular files only.
- Stable TAR metadata:
  - uid/gid `0`.
  - uname/gname empty.
  - constrained file modes.
  - mtime fixed to zero.
- Include `manifest.json` and fragment files.
- Exclude the archive output path if it lives under the dump directory.

## Encrypted Envelope

Create a retriever-specific archive envelope:

```text
magic:      RTRV-PQ-ARCHIVE-v1
header_len: uint32
header:     JSON
frames:     encrypted chunk frames
final:      authenticated final frame
```

Header fields:

```json
{
  "format": "retriever-encrypted-tar-v1",
  "archive": {
    "format": "tar",
    "compression": "none"
  },
  "crypto": {
    "scheme": "hpke",
    "kem": "ML-KEM-1024",
    "kdf": "HKDF-SHA512",
    "aead": "AES-256-GCM",
    "encapsulated_key": "base64..."
  },
  "chunk_size": 1048576
}
```

Encrypt the TAR stream in chunks. Each chunk should authenticate additional data
containing:

- Envelope version.
- Header hash.
- Frame index.
- Final/non-final marker.

Require an authenticated final frame so truncation at a frame boundary is
detected.

## Implementation Sequence

1. Add key file types and `retriever keygen`.
2. Add key load and parse validation.
3. Add deterministic raw TAR packer.
4. Add TAR unpacker with path traversal protections.
5. Add encrypted streaming writer using an HPKE sender context.
6. Add encrypted streaming reader using an HPKE recipient context.
7. Wire `dump -archive-out -recipient`.
8. Add `retriever unpack`.
9. Optionally wire `load -archive -identity`.
10. Add `slog` progress for archive walking, TAR streaming, encryption,
    finalization, decryption, and unpacking.
11. Document key handling and archive workflow.

## Testing Plan

Pure tests should cover:

- TAR path sorting is deterministic.
- TAR metadata does not leak host user/group.
- TAR rejects symlinks, absolute paths, and `..`.
- Encrypted archive header contains no graph names or manifest contents.
- `decrypt(encrypt(tar))` round-trips byte-for-byte.
- Wrong key fails.
- Tampered header fails.
- Tampered frame fails.
- Truncated archive fails.
- Missing final frame fails.

Integration-style tests without a database should cover:

- Create a synthetic dump directory.
- Package and encrypt it.
- Unpack it.
- Compare file tree and checksums.
- Verify `readManifest` still works after unpack.

Database integration can remain separate and gated behind `CONNECTION_STRING`.

## References

- NIST FIPS 203, Module-Lattice-Based Key-Encapsulation Mechanism Standard:
  https://csrc.nist.gov/pubs/fips/203/final
- NIST announcement of finalized post-quantum encryption standards:
  https://www.nist.gov/news-events/news/2024/08/nist-releases-first-3-finalized-post-quantum-encryption-standards
