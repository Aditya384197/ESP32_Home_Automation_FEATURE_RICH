CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'member',
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  token_hash TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS devices (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  owner_id INTEGER NOT NULL,
  token_hash TEXT UNIQUE NOT NULL,
  created_at INTEGER NOT NULL,
  online INTEGER NOT NULL DEFAULT 0,
  last_seen INTEGER NOT NULL DEFAULT 0,
  states TEXT NOT NULL DEFAULT '[0,0,0,0,0]',
  enabled TEXT NOT NULL DEFAULT '[true,true,true,false,false]',
  FOREIGN KEY(owner_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS device_users (
  device_id TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  role TEXT NOT NULL DEFAULT 'operator',
  PRIMARY KEY(device_id,user_id),
  FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE,
  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS commands (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id TEXT NOT NULL,
  relay INTEGER NOT NULL,
  state INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL DEFAULT 0,
  acknowledged_at INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS schedules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id TEXT NOT NULL,
  relay INTEGER NOT NULL,
  hour INTEGER NOT NULL,
  minute INTEGER NOT NULL,
  action INTEGER NOT NULL,
  days INTEGER NOT NULL DEFAULT 127,
  enabled INTEGER NOT NULL DEFAULT 1,
  duration_minutes INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_commands_device ON commands(device_id,id);
CREATE INDEX IF NOT EXISTS idx_commands_pending ON commands(device_id,acknowledged_at,id);
CREATE INDEX IF NOT EXISTS idx_schedules_device ON schedules(device_id,id);
