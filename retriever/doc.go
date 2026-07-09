// Package retriever exports and imports Dawgs graph databases using the
// manifest-based retriever collection format.
//
// The primary database operations accept an already-open graph.Database. The
// package also exposes the collection manifest and fragment structs for callers
// that need to inspect or transform collection contents directly.
//
// Default option constructors mirror the CLI defaults. Callers can attach a
// ProgressFunc to receive structured operation, phase, archive, and periodic
// scan progress events. Validation, compatibility, checksum, metrics, and
// count mismatch failures have typed error values for errors.As checks. Dump,
// Load, Verify, Unpack, and Keygen provide option-based entry points.
//
// Archive APIs are available in both stream-oriented and path-oriented forms.
// Stream-oriented encrypted archive functions use the standard library
// crypto/hpke PublicKey and PrivateKey interfaces. Archive key envelopes can
// also be read from and written to any io.Reader or io.Writer.
package retriever
