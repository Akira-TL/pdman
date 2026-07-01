# Resume diagnostics contract

This document defines the current resume diagnostics contract for pdman v0.6.x. It is written for users, scripts, and agents that need to understand why pdman reused or rejected temporary download state.

## Scope

Resume diagnostics explain the state around `--continue` and temporary files. They are diagnostics, not recovery behavior.

Current boundaries:

- Static downloads can write and read `resume-metadata.json`.
- Dynamic downloads can emit `resume-metadata.json`, but dynamic recovery is not enabled.
- `dynamic-ranges.json` is debug metadata, not the recovery contract.
- Legacy `.pdm` fallback is a compatibility path only.
- History and run JSON outputs expose compact diagnostics for scripts and agents.
- pdman does not automatically repair corrupted partial files.

## Files and outputs

| Surface | Producer | Consumer | Purpose | Contract level |
| --- | --- | --- | --- | --- |
| `resume-metadata.json` | Cache metadata directory for new runs; legacy tmp paths still readable | Resume validation | Recovery identity and segment contract | Strict recovery contract |
| `dynamic-ranges.json` | Cache metadata directory for new runs; legacy tmp paths still readable by explicit path/search-root | `pdman debug ranges` | Runtime range allocator inspection | Debug/diagnostic contract |
| legacy `.pdm` | Older resume path | Legacy fallback | Compatibility only when v2 metadata is missing | Legacy compatibility |
| runtime `history.jsonl` | Runtime history writer | `pdman history`, scripts, agents | Finished task record with resume rejection fields | Stable history record |
| `pdman history --json` | CLI history query | Scripts and agents | Agent-readable history records | CLI JSON contract |
| `pdman history --jsonl` | CLI history query | Stream processors and agents | One enriched history record per line | CLI JSONL contract |
| `pdman run <run_id> --json` | CLI run query | Scripts and agents | One run plus enriched tasks | CLI JSON contract |

## `resume-metadata.json`

`resume-metadata.json` is the strict v2 recovery contract. It is separate from dynamic debug metadata and contains only the information needed to decide whether temporary files are safe to reuse.

Important fields include:

- `schema_version`: currently `2`.
- `kind`: currently `resume`.
- `mode`: `static` or `dynamic`.
- `url`: original URL identity.
- `filename` and `target_path`: target identity.
- `file_size`: expected remote file size.
- `etag` and `last_modified`: optional remote validators.
- `segments`: expected partial file layout and state.

When `--continue` validates v2 metadata, pdman checks URL, target path, file size, etag, last-modified, segment layout, and partial file sizes before reusing temporary state. If validation rejects the metadata, pdman records a resume rejection and does not fall back to legacy `.pdm`.

Changing static chunk options such as `-x` or `-k` does not by itself invalidate existing v2 static resume metadata. pdman follows the segment layout stored in metadata rather than recomputing a new layout from the current options.

## `dynamic-ranges.json`

`dynamic-ranges.json` is debug metadata for dynamic range allocation. It is meant for inspection through `pdman debug ranges` and related readable, JSON, or JSONL output.

It may include allocator diagnostics such as range attempts, selector decisions, fallback reasons, failed ranges, split layout, and last errors. It is not used as the recovery contract and must not be treated as a substitute for `resume-metadata.json`.

## Legacy `.pdm` fallback

The legacy `.pdm` file is retained for compatibility with older temporary directories. It is allowed only when `resume-metadata.json` is missing. When this path is used, pdman emits a legacy fallback warning.

If `resume-metadata.json` exists but is rejected, pdman clears stale temporary files and restarts rather than falling back to `.pdm`. This prevents old metadata from bypassing v2 identity checks.

## Resume rejection format

When resume metadata is rejected, pdman formats the reason as:

```text
Resume rejected [<code>]: <message>
```

The same compact information can also appear in `TaskResult`, runtime history, `pdman history --json`, `pdman history --jsonl`, and `pdman run <run_id> --json`.

JSON diagnostic payloads use this shape:

```json
{
  "present": true,
  "code": "file_size_mismatch",
  "reason": "Resume rejected [file_size_mismatch]: file_size mismatch"
}
```

If no resume rejection happened, the payload is:

```json
{
  "present": false,
  "code": null,
  "reason": null
}
```

## Resume rejection codes

| Code | Meaning |
| --- | --- |
| `unknown` | A fallback code when no more specific rejection code was assigned. |
| `metadata_missing` | The v2 metadata file could not be read. |
| `metadata_json_invalid` | The metadata file was not valid JSON. |
| `metadata_object_invalid` | The parsed metadata was not a JSON object. |
| `schema_version_unsupported` | `schema_version` was not the supported v2 value. |
| `kind_mismatch` | `kind` was not `resume`. |
| `mode_unsupported` | `mode` was not `static` or `dynamic`. |
| `url_mismatch` | Metadata URL did not match the current task URL. |
| `target_path_mismatch` | Metadata target path did not match the current target. |
| `file_size_mismatch` | Metadata file size did not match the current remote file size. |
| `etag_mismatch` | Metadata etag did not match the current remote etag. |
| `last_modified_mismatch` | Metadata last-modified value did not match the current response. |
| `field_invalid` | A required metadata field was missing or had an invalid type/value. |
| `segment_invalid` | A segment entry was malformed or internally inconsistent. |
| `segment_layout_mismatch` | Segment ordering, indexes, coverage, or expected layout did not match. |
| `partial_too_large` | An on-disk partial file was larger than its expected segment size. |

These codes are intended for diagnostics and script branching. They should not be interpreted as download failure reasons. A task can reject old resume metadata, restart, and still complete successfully.

## Recommended reading paths

Use the narrowest surface that answers the question.

### For scripts checking recent task outcomes

Use:

```bash
pdman history --json
```

This returns:

- `count`: number of matching records.
- `records`: history records enriched with `resume_rejection`.

Use `--jsonl` when a stream of independent records is easier to process:

```bash
pdman history --jsonl
```

Each line is one enriched history record. Every record contains a `resume_rejection` payload.

### For agents inspecting one run

Use:

```bash
pdman run <run_id> --json
```

This returns:

- `run`: the run summary loaded from the runtime run file.
- `tasks`: history task records belonging to the run, each enriched with `resume_rejection`.
- `task_count`: number of tasks in the response.

This is the preferred agent entrypoint when a user asks why a specific run behaved a certain way.

### For humans inspecting recent history

Use:

```bash
pdman history
pdman run <run_id>
```

Human output remains readable and includes resume rejection text when present. Scripts should prefer JSON/JSONL instead of parsing human output.

### For direct resume metadata inspection

Use:

```bash
pdman debug resume --metadata /path/to/resume-metadata.json
pdman debug resume --metadata /path/to/resume-metadata.json --json
pdman debug resume --metadata /path/to/resume-metadata.json --jsonl
pdman debug resume --metadata /path/to/resume-metadata.json --state partial --json
pdman debug resume --latest --search-root /path/to/tmp --json
```

This command reads `resume-metadata.json`, validates it, refreshes current partial sizes for its segments, and renders readable, JSON, or JSONL diagnostics. It does not modify metadata or partial files and does not recover downloads. With `--latest`, it scans the cache root for the newest valid `resume-metadata.json`. If `--cache-dir` is provided, that cache dir is the only default root. If `--search-root` is provided, that directory becomes the strict search boundary and default roots are not added. Invalid metadata candidates are skipped during latest discovery. With `--state`, readable output shows filter and filtered stats, JSON output includes `filter`, `count`, and `filtered_stats`, and JSONL emits only matching segments. `--metadata` and `--latest` are mutually exclusive source selectors.

### For dynamic allocator debugging

Use:

```bash
pdman debug ranges --json
pdman debug ranges --jsonl
```

This inspects `dynamic-ranges.json`, not resume recovery state. New runs write it to the cache metadata directory rather than task tmp. `--latest` follows the same cache/search-root boundary rules as `debug resume` and skips invalid candidates. Use it when debugging dynamic range selection, split behavior, attempts, or fallback reasons. Do not use it to decide whether temporary files are safe to reuse.

## Stable fields for scripts and agents

The following fields are the intended stable diagnostics surface:

- History records: `resume_rejection_code`, `resume_rejection_reason`.
- Enriched JSON records: `resume_rejection.present`, `resume_rejection.code`, `resume_rejection.reason`.
- Run detail JSON: `run`, `tasks`, `task_count`.
- Resume inspect JSON: `source_path`, `schema_version`, `kind`, `mode`, `url`, `filename`, `target_path`, `file_size`, `etag`, `last_modified`, `created_at`, `updated_at`, `filter`, `count`, `stats`, `filtered_stats`, `segments`.
- Resume inspect segment JSON: `index`, `start`, `end`, `path`, `expected_size`, `existing_size`, `state`.
- Resume rejection text format: `Resume rejected [<code>]: <message>`.

The full `resume-metadata.json` file is an internal recovery contract. Scripts may inspect it for debugging, but should not rely on every field unless the field is documented here.

## Non-goals in v0.6.x diagnostics

The diagnostics surfaces above do not imply the following behaviors:

- No dynamic recovery is enabled yet.
- No corrupted partial file repair is performed.
- No full resume metadata dump is exposed through history or run JSON.
- No legacy `.pdm` migration is performed.
- No resume rejection changes task exit code by itself.
