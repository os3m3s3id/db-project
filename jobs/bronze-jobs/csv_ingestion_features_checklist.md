# CSV → Bronze Ingestion — General Feature Checklist

A tool-agnostic checklist of the design patterns used, for reuse as a template on any future load structure regardless of stack.

## 1. Persisted Schema Design
- Keep the number of *real, persisted* tables minimal and explicit (e.g. one for clean data, one for chunk-level run state, one for rejected rows)
- Everything else used mid-process is transient/intermediate (a view, temp object, or in-memory structure) — not part of the stored schema
- Table/destination creation is idempotent (safe to run every time without erroring or duplicating)

## 2. Resumability / Idempotency
- Maintain a state log keyed by (source, chunk) recording status (in progress / success / failed)
- Before processing, check which chunks are already completed and skip them
- A rerun after a partial failure should only redo the missing work, not the whole load
- State writes use an upsert pattern (insert, or update if the key already exists) so re-attempts overwrite cleanly instead of erroring or duplicating

## 3. Chunked Processing
- Process the source in fixed-size batches rather than loading everything into memory at once
- Compute number of chunks from total row count and chunk size
- Each chunk is pulled and processed independently (offset/limit or equivalent), at its own granularity in the state log

## 4. Manual Row-Level Validation
- Don't fully trust the default parser's error handling — validate structure yourself where correctness matters (e.g. detect rows with unexpected extra content vs. harmless trailing empties)
- Separate rows into "valid" and "invalid" before further processing

## 5. Dead Letter Queue (DLQ) for Rejects
- Invalid/malformed rows are never silently dropped — they're written to a dedicated persisted reject store (a DLQ)
- Each rejected record is logged with enough context to diagnose later: source identifier, originating file, the raw content, a reason/error message, and a timestamp
- Keeps bad data isolated and inspectable without blocking or corrupting the main load

## 6. Transactional Writes (per chunk)
- Wrap each chunk's write in a transaction: begin → attempt → commit, or roll back on failure
- Record state as "in progress" before writing, then update to "success" or "failed" after
- On failure, roll back the data write but still persist a failure record — failures should be visible, not silent

## 7. Data Enrichment
- Add processing metadata to each record before storing (e.g. chunk identifier, ingestion timestamp)
- Apply consistent typing to these added fields

## 8. Dual Output
- Write the same processed chunk to more than one destination when useful (e.g. a queryable store and a portable file format) so downstream consumers have flexibility

## 9. Retry Logic
- Wrap the write step in a retry loop with a capped number of attempts
- Use backoff between attempts (e.g. linearly increasing wait time)
- On permanent failure after all retries, log it clearly and let the run continue rather than crashing entirely — the failure is captured in state, not swallowed

## 10. Cleanup
- Remove intermediate/transient objects at the end of a run so only the intended persisted destinations remain
- Keeps the schema inspectable and free of clutter between runs

## 11. Observability / Logging
- Log at each major step: what was skipped, how many rows staged, how many rejected, per-chunk success, retry warnings, final summary
- Return a run summary (e.g. new rows this run, total rows now stored) so the caller/orchestrator can track outcomes

## 12. Source Identification
- Derive a stable identifier for each source (e.g. from its filename/path)
- Use this identifier as the key across state and reject (DLQ) logs so multiple sources can share the same destinations without collision
- Also capture a content fingerprint (e.g. hash, size + mtime) so a changed file reusing an old name is not silently skipped

## 13. Schema / Header Validation
- Validate the header before processing: missing columns, unexpected extra columns, reordered columns, renamed columns
- Decide and document the policy for each case (fail fast, ignore extras, map by name not position)
- Detect schema drift between runs of the same source and surface it rather than loading misaligned data
- Keep the Bronze destination tolerant (raw values as text) so downstream typing failures don't block ingestion

## 14. Encoding, Delimiter & Parsing Robustness
- Handle BOM and non-UTF-8 encodings explicitly (detect or configure; don't assume)
- Support quoted fields containing the delimiter, embedded newlines, and escaped quotes
- Validate the delimiter/dialect rather than trusting a default
- Treat empty files, header-only files, and zero-row chunks as valid no-ops, not errors

## 15. Type Coercion Policy
- A row can be structurally valid but have bad values (text in a numeric field, unparseable dates)
- Define the policy: keep raw-as-text in Bronze, null-and-flag, or route to DLQ
- Never let a single bad value abort a whole chunk unless that is the documented intent

## 16. Duplicate Handling
- Define a natural/business key and dedup policy for rows within a file and across re-ingested files
- Decide whether re-ingesting the same file appends, replaces, or is rejected by the content fingerprint (see §12)

## 17. File Lifecycle & Discovery
- Define how sources are discovered (glob/prefix), processing order, and handling of late-arriving files
- Guard against files still being written / locked / growing at job start (size-stable check, staging dir, or done-marker)
- Move or mark processed files (archive/processed prefix) so they are not re-scanned indefinitely

## 18. Memory Safety of Chunk Planning
- Computing chunk count from a full row count (§3) must not require reading the whole file into memory
- Stream to count, use file size heuristics, or page without a precomputed total

## 19. Concurrency & Locking
- Prevent two job instances from processing the same source or writing the same state rows simultaneously
- Use a lock (advisory lock, row lock, lease, or single-flight orchestration) and define behavior when the lock is held

## 20. Failure Thresholds & Poison Chunks
- Abort or alert if the reject rate for a source exceeds a threshold (e.g. >X% of rows) instead of loading a mostly-corrupt file
- A chunk that fails every retry across multiple runs (poison chunk) is quarantined and skipped so it can't block the pipeline forever
- Quarantined chunks are visible in state and require explicit reprocessing

## 21. Cross-Destination Consistency
- With dual output (§8), the queryable store and the file output can diverge on partial failure
- Declare one destination as source of truth and reconcile the other, or write both under one atomic outcome per chunk

## 22. State & DLQ Maintenance
- Define retention, partitioning, and pruning for the state log and DLQ so they don't grow unbounded
- Plan for schema migration of the persisted Bronze table itself over time (additive changes, versioning)

## 23. Config Validation & Fail-Fast
- Validate all configuration (paths, credentials, destination names, batch size, retry settings, expected schema) before touching any data
- Fail immediately with a clear message on bad/missing config

## 24. Time Handling
- Use UTC for all ingestion timestamps and record the timezone explicitly
- Don't rely on local server clock semantics for ordering or partitioning

## 25. Test Fixtures
- Maintain fixtures for each failure mode above: malformed rows, wrong encoding, bad delimiter, missing/extra/reordered columns, embedded newlines, empty file, duplicates, oversized file
- Keep a golden-file regression test asserting row counts, reject counts, and enrichment output

---

### General template pattern to reuse for the next loader
1. Define configuration up front (paths/locations, destination names, batch size, retry settings, expected schema) and validate it, failing fast on anything bad
2. Discover sources; skip files that are still being written; acquire a lock so no other instance processes the same source
3. Ensure persisted destinations exist (idempotent setup)
4. Identify the source (stable id + content fingerprint); check state log to determine what's already done (resumability, dedup)
5. Validate header/schema, encoding, and delimiter; treat empty/header-only sources as no-ops
6. Validate/stage the raw source; route rejects to a DLQ with diagnostic context; abort or alert if the reject rate exceeds threshold
7. Loop through batches: mark in-progress (upsert) → write transactionally → enrich with metadata (UTC timestamps) → write to destination(s) → retry with backoff on failure → mark success/failed; quarantine poison chunks
8. Reconcile dual outputs against the source-of-truth destination
9. Clean up any transient/intermediate objects; archive/mark the processed file
10. Return a summary of the run
11. Periodically prune/partition state and DLQ; version the Bronze schema for additive changes
