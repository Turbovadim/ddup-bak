# ddup-bak

very experimental archive/dedup format that can use multiple different compression formats per file/chunk

repositories written before 0.11 use archive format version 1 and are migrated to version 2 in place the first time 0.11 or later opens them, see ARCHIVE.md.

0.11 changes the C API (callbacks and the functions taking them gained a `user_data` argument), so C programs built against 0.10 must be rebuilt, not just pointed at the new library.

custom `ChunkStorage` implementations must implement `sync`: `ChunkStorageLocal` no longer flushes each chunk as it writes it, so a wrapper around it has to forward `sync`, and a storage whose writes are already durable can return `Ok(())`.
