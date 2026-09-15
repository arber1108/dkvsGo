# dkvsGo

A log-structured key-value store written in Go, inspired by Bitcask.

## How it works

- Writes are appended to a segment log file (`logfiles/NNNN.log`) as
  length-prefixed, checksummed entries. Deletes append a tombstone
  entry rather than mutating existing data.
- An in-memory hash table maps each key to `(segment ID, offset)` so
  reads are a single seek instead of a log scan.
- Segments roll over once they hit a size or entry-count limit.
- On startup, `recoverStore` replays every segment to rebuild the hash
  table from disk, since the index itself isn't persisted.
- `Compact` merges old (inactive) segments into one, dropping
  tombstones and superseded values, to reclaim disk space.


## Running

```
go run .
go test ./...
```
