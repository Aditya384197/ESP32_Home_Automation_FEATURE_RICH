ALTER TABLE commands ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE commands ADD COLUMN acknowledged_at INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_commands_pending ON commands(device_id,acknowledged_at,id);
