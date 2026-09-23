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
