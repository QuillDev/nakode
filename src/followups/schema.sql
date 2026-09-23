CREATE TABLE IF NOT EXISTS followup_inboxes (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    paused INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS followup_batches (
    batch_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    cutoff INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('claimed', 'dispatching', 'consumed')),
    provider_turn_id TEXT,
    blocked_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_unsettled_followup_batch
    ON followup_batches(session_id) WHERE state <> 'consumed';
CREATE TABLE IF NOT EXISTS followup_messages (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    message_id TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    prompt_json TEXT NOT NULL,
    display_text TEXT NOT NULL,
    attachment_labels_json TEXT NOT NULL,
    request_digest BLOB NOT NULL,
    payload_bytes INTEGER NOT NULL,
    batch_id TEXT REFERENCES followup_batches(batch_id),
    UNIQUE(session_id, message_id)
);
CREATE INDEX IF NOT EXISTS followup_pending_order ON followup_messages(session_id, sequence);
CREATE TABLE IF NOT EXISTS followup_receipts (
    command_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    digest BLOB NOT NULL,
    resource_id TEXT
);

-- Receipt survives child/report deletion so queued evidence never loses its trusted origin.
CREATE TABLE IF NOT EXISTS child_followup_deliveries (
    report_sequence INTEGER PRIMARY KEY,
    message_sequence INTEGER NOT NULL UNIQUE REFERENCES followup_messages(sequence) ON DELETE CASCADE
);

-- Runtime-owned metadata is separate from all producer-controlled prompt payloads.
CREATE TABLE IF NOT EXISTS followup_sources (
    message_sequence INTEGER PRIMARY KEY REFERENCES followup_messages(sequence) ON DELETE CASCADE,
    source_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS followup_batch_display (
    batch_id TEXT PRIMARY KEY REFERENCES followup_batches(batch_id) ON DELETE CASCADE,
    coordination_json TEXT NOT NULL
);

-- Existing runtime receipt rows retain their inert child provenance on upgrade.
INSERT OR IGNORE INTO followup_sources(message_sequence, source_json)
SELECT d.message_sequence, json_object('kind', 'durable_child_evidence',
    'sessionId', r.child_id, 'title', substr(l.child_title, 1, 120),
    'callId', NULL, 'status', r.state)
FROM child_followup_deliveries d JOIN session_child_reports r ON r.sequence = d.report_sequence
JOIN session_child_links l ON l.child_id = r.child_id;

CREATE UNIQUE INDEX IF NOT EXISTS followup_source_call
ON followup_sources(json_extract(source_json, '$.sessionId'), json_extract(source_json, '$.callId'))
WHERE json_extract(source_json, '$.callId') IS NOT NULL;
