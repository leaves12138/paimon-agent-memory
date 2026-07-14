# Paimon AI Chat Collector

A small Java 11 daemon that incrementally copies Codex and Claude Code conversations into Apache
Paimon. Version 1 stores the current session list and the raw conversation stream; it does not
perform memory retrieval, summarization, preference extraction, or vector indexing.

## Storage model

The collector creates exactly two unpartitioned tables.

### `ai_chat_sessions`

This is a normal primary-key table containing the current sidebar state. Its primary key is
`(source_type, session_id)`. It is not a Data Evolution or BLOB table.

| Column | Type | Purpose |
| --- | --- | --- |
| `source_type` | STRING | `codex` or `claude` |
| `session_id` | STRING | Native client session ID |
| `title` | STRING | Current session title |
| `cwd` | STRING | Project working directory |
| `archived` | BOOLEAN | Current archive state |
| `source_path` | STRING | Transcript path |
| `source_cursor` | STRING | Committed byte cursor, file identity, and line anchor |
| `last_commit_id` | BIGINT | Last cross-table commit identifier |
| `pending_commit_id` | BIGINT | Unfinished two-phase message commit, otherwise null |
| `pending_cursor` | STRING | Fixed end boundary of the unfinished commit |
| `created_at` | TIMESTAMP(3) | Source creation time |
| `updated_at` | TIMESTAMP(3) | Source update time |
| `last_message_at` | TIMESTAMP(3) | Last collected message time |
| `ingested_at` | TIMESTAMP(3) | Collector update time |
| `subagent_source_json` | STRING | Native Codex subagent metadata; null for visible root sessions and Claude |

### `ai_chat_messages`

This is an append-only Data Evolution BLOB table with row tracking and `bucket=-1`. It has no
primary key and no partition.

| Column | Type | Purpose |
| --- | --- | --- |
| `message_id` | STRING | Deterministic source identity |
| `source_type` | STRING | `codex` or `claude` |
| `session_id` | STRING | Owning session |
| `sequence_no` | BIGINT | Source JSONL byte position |
| `role` | STRING | User, assistant, developer, system, or tool |
| `event_type` | STRING | Source event kind |
| `content_json` | STRING | Original event JSON plus the attachment manifest |
| `attachments` | ARRAY\<BLOB\> | Ordered attachment bytes for this event |
| `created_at` | TIMESTAMP(3) | Source event time |
| `ingested_at` | TIMESTAMP(3) | Collector ingestion time |

Binary Base64 values are replaced in `content_json` by `paimon-blob:<index>`. The corresponding
bytes are written once to `attachments[index]`. Missing or oversized files retain a null array
element and an explanatory manifest entry, so later elements keep their original indexes.

## Configuration

The two configuration files are intentionally never merged.

`paimon.properties` contains only options used to construct the Paimon Catalog:

```properties
metastore=rest
uri=http://127.0.0.1:8080
warehouse=default
```

Version 1 supports only the REST Catalog and therefore requires `metastore=rest`; the legacy
`type=rest` spelling is rejected. Authentication and filesystem options also belong in this file.

`project.properties` contains all collector settings:

```properties
database=ai_memory
sessions.table=ai_chat_sessions
messages.table=ai_chat_messages

collector.scan.interval=5m
collector.commit.interval=5m
collector.run-once=false
collector.id=replace-with-a-unique-installation-id

collector.codex.enabled=true
collector.codex.path=~/.codex
collector.claude.enabled=true
collector.claude.path=~/.claude

attachments.enabled=true
attachments.download-remote=false
attachments.max-size=100MB

collector.buffer.max-records=10000
collector.scan.max-records-per-source=10000
collector.retry.max-attempts=10
collector.retry.initial-delay=5s

dashboard.enabled=true
dashboard.host=127.0.0.1
dashboard.port=8787
dashboard.page-size=25
dashboard.max-page-size=100
dashboard.max-scan-rows=50000
dashboard.max-attachment-preview-size=10MB
```

Use one stable `collector.id` for the lifetime of a session/messages table pair. The value is
recorded as table ownership metadata and is also used to derive Paimon commit users. Version 1
permits exactly one active writer for a table pair: a different ID is rejected, while any second
local writer for that table pair is blocked before initialization. A failover collector may reuse the owning ID, but the
operator must ensure the old host is stopped first because the local lock cannot coordinate hosts.
Independent collectors need different database or table names. Restore is read-only and may open
the tables with a different configured ID.

The scan and commit intervals accept `ms`, `s`, `m`, `h`, `d`, or ISO-8601 duration syntax.
Attachment sizes accept `B`, `KB`, `MB`, or `GB`. `collector.scan.max-records-per-source` is an
internal per-read chunk size, not a per-wake-up limit. Each wake-up freezes the current transcript
set and byte EOFs, then repeats bounded reads until every enabled source reaches those boundaries;
records appended while it is running belong to the next wake-up. The commit interval is the regular flush cadence;
reaching `collector.buffer.max-records` triggers an earlier commit, then the same wake-up continues
scanning until it has caught up. Startup queues one immediate flush after the initial catch-up, so a
large existing history does not leave its final partial chunk waiting for the first commit interval.

## Source behavior

Codex session metadata comes from `~/.codex/state_5.sqlite`, and transcripts come from each
`threads.rollout_path`. The collector stores canonical `response_item.message` events, tool calls,
and generated images. It deliberately ignores duplicate `event_msg.user_message` and
`event_msg.agent_message` projections. Codex images, generated images, and files under
`~/.codex/attachments` are copied into the BLOB array. Native Codex subagent source metadata is
stored on the session row so hidden worker threads remain attached to their parent task instead of
appearing as duplicate sidebar conversations.

Claude Code transcripts are discovered from top-level files under
`~/.claude/projects/<encoded-project>/<session-id>.jsonl`. `sessions-index.json` is optional title
metadata, not the source of truth. Main transcripts are collected; `subagents/agent-*.jsonl` files
are intentionally excluded from version 1. Base64 image/document blocks and explicit local file
references are moved to the BLOB array.

Every session has an independent byte cursor. Only newline-terminated JSONL records advance the
cursor. If a source file is replaced, truncated, or rewritten in place, the collector verifies and
searches for the last committed line anchor. If the anchor has disappeared, that session is paused
instead of blindly replaying an append table and creating duplicates. One unreadable session or a
temporarily unavailable remote attachment does not prevent other sessions from advancing.

## Dashboard

The distribution includes a loopback-only, read-only web dashboard for customer demonstrations
and local inspection. With `dashboard.enabled=true`, an ordinary `bin/paimon-agent start` starts
the collector and Dashboard in the same managed process; `bin/paimon-agent stop` stops both. Set
`dashboard.enabled=false` to run only the collector. A one-shot
`bin/paimon-agent run --once` always performs one collection cycle and exits without starting the
Dashboard, regardless of this setting.

The conversation view merges the collector's current in-memory batch with rows already visible in
Paimon. Sessions with a new local increment are marked `有待提交更新`, while individual messages
are marked `待上传` or `已上传`. Pending attachment bytes can be previewed through the same
size-limited, loopback-only endpoint. Codex subagent threads are hidden from the session sidebar,
matching the native Codex task list. A standalone Dashboard has no access to another process's
in-memory batch and therefore shows Paimon rows only.

After starting the service, open the Dashboard directly with either loopback URL:

```text
http://127.0.0.1:8787/
http://localhost:8787/
```

No token is required. `bin/paimon-agent dashboard-url` remains available as a convenience command
that prints the configured local address.

The overview distinguishes uploaded rows from sessions and messages that still belong to an
unfinished collector commit. The detail area follows a two-pane chat layout: searchable Codex and
Claude sessions are listed on the left, and selecting one opens its chronological conversation on
the right. Older messages are loaded in bounded pages without losing the current scroll position.
Message JSON and attachment metadata are loaded only after an explicit detail click; supported
images are then fetched from Paimon only when the user opens their preview. The preview is bounded by
`dashboard.max-attachment-preview-size`, while `dashboard.page-size`,
`dashboard.max-page-size`, and `dashboard.max-scan-rows` bound interactive queries. On narrow
screens the two panes become a session-list-to-chat navigation flow.

Session and message list reads push equality filters into Paimon, exclude attachment BLOB bytes,
and keep a bounded five-minute in-process query cache. Collector commits invalidate the cache, and
the refresh controls force an immediate reload. The first page loads only the session list;
overview statistics and message history no longer compete for the same initial request. Data
Evolution message scans use smaller read-only splits so indexed rows spread across historical files
can be read in parallel. `ai_chat_messages` enables Morax-managed incremental BTree indexing and
compaction for `session_id` through the table options `morax.btree-index.enabled=true` and
`global-index.btree.index-column=session_id`. The Dashboard reads with
`global-index.search-mode=full`, so rows appended after the latest asynchronous index commit remain
visible through a raw-data fallback. The index is an optimization, not a correctness requirement.

To inspect the Paimon tables without running the collector, start a standalone read-only Dashboard
in the foreground:

```bash
bin/paimon-agent dashboard
```

This command does not acquire the collector writer lock or collect, upload, restore, or modify any
conversation. Open the configured loopback address after it starts. Do not run the standalone
command on the same host and port while the Dashboard managed by `start` is already active.

The server binds only to the literal loopback addresses `127.0.0.1` or `::1` and also accepts
`localhost` as a request Host alias; it cannot be bound to `0.0.0.0` or another network interface.
Do not expose conversation history directly over a LAN or the public Internet. For a remote
demonstration, keep the server on loopback and forward the same port through SSH:

```bash
ssh -L 8787:127.0.0.1:8787 user@collector-host
```

Then open `http://127.0.0.1:8787/` or `http://localhost:8787/` in the local browser. Use the same
local and remote port so the Dashboard's strict Host validation is preserved.

## Build

This project currently depends on the `ARRAY<BLOB>` implementation introduced by Apache Paimon
PR #8181 plus the local follow-up on `codex/list-blob-support`. Install that branch first, using
JDK 11:

```bash
cd <path-to-incubator-paimon2>
git switch codex/list-blob-support
export JAVA_HOME=<path-to-jdk-11>
export PATH="$JAVA_HOME/bin:$PATH"
mvn -pl paimon-bundle,paimon-filesystems/paimon-oss -am -DskipTests install
```

This version was verified against local Paimon commit `56b3c213ce87318c0d099a396eabf5964a6df8cd`
on that branch.

Then build and test the collector:

```bash
cd <path-to-paimon-agent>
mvn clean package
```

The build targets Java 11. No Maven `-s` or `-gs` option is required.

### Distribution package

`mvn package` also creates `target/paimon-agent-<version>-dist.tar.gz`. It is a relocatable
installation package with the following layout:

```text
paimon-agent-<version>/
├── bin/paimon-agent
├── config/paimon.properties
├── config/project.properties
├── data/
└── lib/
    ├── paimon-agent-<version>.jar
    └── all runtime dependency JARs
```

The package does not contain a JDK. Install JDK 11 and set `JAVA_HOME` before using it. Edit the
two files under `config/`, especially the Catalog endpoint and installation-unique `collector.id`.
Catalog credentials can be placed in `paimon.properties`, so both configuration files are packaged
with mode `0600`; the `config/` and `data/` directories use mode `0700`. Tar ownership is normalized
to `root:root`; when a non-root user extracts the package, the files are owned by that user instead.

When upgrading a version-1 installation whose `ai_chat_sessions` table predates
`subagent_source_json`, stop the old daemon and start the new collector first with the same
`collector.id`. The owning writer validates both tables, adds the nullable column, and backfills
Codex metadata. Run standalone `dashboard` or `restore` commands only after that writer upgrade has
completed; read-only commands deliberately never alter the table schema.

The control script keeps the PID, process log, exact pending-commit WAL, and local restore/cache
data under `data/`:

```bash
bin/paimon-agent start
bin/paimon-agent status
bin/paimon-agent stop
bin/paimon-agent run --once
bin/paimon-agent dashboard
bin/paimon-agent dashboard-url
```

The script applies an owner-only umask before creating runtime files. `start` and `stop` are
serialized by `data/.paimon-agent-control.lock`, and the PID file is published atomically. A second
control command waits up to 40 seconds by default; change this with
`PAIMON_AGENT_CONTROL_TIMEOUT`. If a launcher is killed before it can remove the control lock,
verify that the PID recorded in its `owner` file no longer exists before removing the stale lock
directory.

Restore one or all sessions into the native Codex or Claude directory with:

```bash
# Stop the destination client before running either command.
bin/paimon-agent restore --type codex
bin/paimon-agent restore --type codex --target /tmp/codex-home --target-project /path/to/project
bin/paimon-agent restore --type claude --target-project /path/to/local/project
bin/paimon-agent restore --type claude --target ~/.claude --session-id <id> --overwrite
```

For Codex, start and stop Codex once on the destination machine before the first restore. The
restore command requires Codex's officially initialized `~/.codex/state_5.sqlite`; it will not
create a partial replacement database. It writes a native rollout JSONL and a matching sidebar
row. The original `response_item` events remain intact, while the restore adds deterministic
`task_started`, `user_message`, `agent_message`, and `task_complete` wrappers so Codex can rebuild
browsable turns. When `--overwrite` is used, the old rollout is privately backed up and restored if
the SQLite update fails. `--target-project` rewrites the restored cwd; otherwise an existing source
cwd is reused, with the target Codex home's parent as the safe fallback. Selecting a visible Codex
task also restores all of its stored subagent descendants, writes their native `source` metadata
and spawn edges, and keeps those worker threads hidden from the destination sidebar. Supplying a
subagent ID resolves to its complete visible root task. Restored multi-agent tasks retain Codex V2
metadata, root-session identity, direct-parent links, and a native context-window identifier;
pending parent branches are skipped together so no orphan worker thread is installed.

For Claude, the default target honors `CLAUDE_CONFIG_DIR` and otherwise uses `~/.claude`.
`--target-project` rewrites the remote `cwd` to a project path on this machine and places the
transcript in Claude's matching encoded project directory. If it is omitted, an existing source
`cwd` is reused; otherwise the current working directory is used.

Existing local sessions are skipped unless `--overwrite` is present. A Paimon session with an
unfinished cross-table commit is also skipped so a partial history cannot become the local copy;
run the collector again and retry the restore. After streaming messages, restore re-reads the
session checkpoints; if a selected conversation changed concurrently, it stops before installing
client history and asks the user to retry. Restore staging is kept under `data/restore/` after a
failure for diagnosis and removed after success.

Inline attachments are rebuilt as native Base64/data URIs. Stored local or downloaded remote
files are restored under the destination client's private restored-attachments directory using
content-addressed names, without replacing a different existing file. If an attachment was not
collected (for example because it exceeded `attachments.max-size`), its placeholder and diagnostic
metadata are retained rather than fabricating an empty attachment.

This first version restores recognizable, browsable conversation history. It does not recreate
Codex execution context that the collector intentionally does not store, such as `turn_context`,
tool runtime state, or resumable in-flight work. Restart Codex or Claude Code after restoration.

The script validates that the selected Java runtime is version 11. Configuration and data paths
can be overridden with `PAIMON_AGENT_PAIMON_CONFIG`, `PAIMON_AGENT_PROJECT_CONFIG`, and
`PAIMON_AGENT_DATA_DIR`. The launcher uses a conservative `-Xms256m -Xmx1024m` heap by default so
the daemon does not reserve a workstation-sized heap. Additional JVM options (including an
overriding `-Xmx`) can be supplied through `PAIMON_AGENT_JAVA_OPTS`. The
start/stop control-lock wait and graceful-stop wait can be changed with
`PAIMON_AGENT_CONTROL_TIMEOUT` and `PAIMON_AGENT_STOP_TIMEOUT`, respectively.

## Run

Extract the distribution, edit its configuration, and use the control script:

```bash
tar -xzf target/paimon-agent-0.1.0-SNAPSHOT-dist.tar.gz -C target
cd target/paimon-agent-0.1.0-SNAPSHOT
$EDITOR config/paimon.properties config/project.properties
bin/paimon-agent start
```

To test one scan and commit without starting the scheduler:

```bash
bin/paimon-agent run --once
```

Alternative configuration paths can be supplied through `PAIMON_AGENT_PAIMON_CONFIG` and
`PAIMON_AGENT_PROJECT_CONFIG`.

## Commit semantics

Before touching Paimon, the collector atomically freezes the complete batch—including attachment
bytes—in `data/pending/pending-commit.bin`. Each commit then uses three steps: first write
`pending_commit_id` and `pending_cursor` to the session table, then append messages, and finally
promote the pending cursor to `source_cursor`. Both tables use stable Paimon stream commit users and
monotonically increasing commit identifiers. An uncertain or partial failure is retried through
`filterAndCommit` with the same immutable batch and identifier. The owner-private WAL is removed
only after the Paimon commit succeeds. Its header binds it to the collector and the REST
Catalog/database/table-pair identity, and a SHA-256 checksum rejects truncated or corrupted WAL
content instead of replaying it elsewhere.

After a crash, the collector loads and replays the exact local WAL without depending on mutable
source files. If a legacy or manually removed WAL is absent while Paimon contains pending session
rows, it falls back to scanning only those sessions and stops exactly at the durable
`pending_cursor`. Content appended after the failed batch is left for the next commit. A
multi-session recovery is never committed partially by the timer or shutdown path. Once any commit
attempt has started, scanning stays frozen until that exact batch succeeds, including across a
process restart when the WAL is present.
