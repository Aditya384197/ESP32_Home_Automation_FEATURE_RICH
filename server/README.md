# Cloud backend

Cloudflare Workers + D1 reference backend for the ESP32 Smart Home firmware.

## Reliability changes in this revision

- Remote relay commands carry a database ID and remain pending until the ESP32 acknowledges them.
- Pending commands expire after 5 minutes so stale manual actions are not replayed after a long outage.
- The ESP32 sends `ackIds` with its next successful poll.
- Schedule replacement uses a D1 batch transaction sequence.
- New user passwords use PBKDF2-HMAC-SHA-256 with a per-password salt.
- Legacy salted SHA-256 password records remain readable for migration compatibility.

## Deployment

Apply the schema to a new database, or run the migrations in order against an existing database.

Required Worker secrets/variables include:

- `ADMIN_EMAIL`
- `ADMIN_PASSWORD`
- optional `SESSION_TTL_SECONDS`

The ESP32 should be configured with the Worker HTTPS base URL, a provisioned device ID, and the device token returned when the device is created.

OTA is intentionally not part of this revision.
