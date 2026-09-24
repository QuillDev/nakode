-- Relationship metadata is not a second parent authority. Links remain canonical.
CREATE TABLE IF NOT EXISTS session_relationship_revisions (
    child_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision >= 0)
);
INSERT OR IGNORE INTO session_relationship_revisions(child_id, revision)
SELECT child_id, 1 FROM session_child_links;
CREATE TRIGGER IF NOT EXISTS child_link_insert_revision AFTER INSERT ON session_child_links
BEGIN
    INSERT INTO session_relationship_revisions(child_id, revision) VALUES (NEW.child_id, 1)
    ON CONFLICT(child_id) DO UPDATE SET revision = revision + 1;
END;
CREATE TRIGGER IF NOT EXISTS child_link_update_revision AFTER UPDATE OF parent_id ON session_child_links
WHEN OLD.parent_id <> NEW.parent_id
BEGIN
    UPDATE session_relationship_revisions SET revision = revision + 1 WHERE child_id = NEW.child_id;
END;
-- Parent deletion may orphan a retained child. Its fence must never return to zero (ABA).
CREATE TRIGGER IF NOT EXISTS child_link_delete_revision AFTER DELETE ON session_child_links
BEGIN
    UPDATE session_relationship_revisions SET revision = revision + 1 WHERE child_id = OLD.child_id;
END;
CREATE TABLE IF NOT EXISTS session_relationship_transitions (
    command_key TEXT PRIMARY KEY,
    digest BLOB NOT NULL,
    child_id TEXT NOT NULL,
    previous_parent_id TEXT,
    parent_id TEXT NOT NULL,
    previous_revision INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    source_call_id TEXT NOT NULL,
    changed_at_ms INTEGER NOT NULL,
    UNIQUE(parent_id, source_call_id)
);
