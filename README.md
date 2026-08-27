# ESP32 Smart Home — 24/7 Stability Revision

This revision keeps the original offline-first smart-home architecture and focuses on reliability, recovery, storage integrity, cloud resilience, safer JSON handling, and long-running operation.

**OTA is intentionally not included in this revision.** The existing factory-only partition table is therefore preserved. Do not flash an OTA partition table with this source revision.

## What was hardened

### Local control
- Relay state changes are persisted with a coalescing save task so rapid toggles do not cause a flash write for every transition.
- Remote, schedule, physical-switch and HTTP relay changes only trigger a persistence write when the logical state actually changes.
- Relay configuration/state snapshots are protected by the existing mutexes.
- JSON request parsing uses cJSON rather than fragile substring parsing.
- JSON responses containing user-configurable strings are generated through cJSON where escaping matters.
- Captive portal DNS remains local-only and does not depend on Internet access.

### Wi-Fi resilience
- AP + STA operation remains unchanged.
- STA reconnects are handled by a dedicated task with bounded backoff rather than repeatedly calling `esp_wifi_connect()` directly from the Wi-Fi event callback.
- Manual offline mode stops reconnect attempts until the user enables connectivity again.
- STA configuration can still be changed without a full restart.

### Cloud resilience
- Cloud polling is reduced to a 5-second normal interval.
- Cloud failures use bounded exponential backoff up to 60 seconds instead of hammering a failed network/service.
- Oversized/truncated HTTP responses are rejected rather than treated as successful responses.
- HTTPS is required for the cloud endpoint.
- Cloud request buffers are reused per request without the previous 16 KiB heap capture allocation.
- Command delivery now uses explicit command IDs and acknowledgements. A command is not deleted merely because the server returned it; the device acknowledges it on the next successful poll.
- Commands have a 5-minute expiry to prevent stale remote relay actions after long outages.
- Schedule replacement on the server is performed as a D1 batch operation rather than a long sequence of independent writes.
- Cached schedules remain local and continue to execute when the cloud is unavailable.

### Authentication/backend
- New passwords are stored using PBKDF2-HMAC-SHA-256 with a per-password salt and 100,000 iterations.
- Existing legacy `salt$SHA256` password records remain readable so an existing deployment is not locked out; new passwords use the stronger format.
- Login/session input validation is stricter.
- Device command authorization and device-token hashing remain in place.

## Build

The project continues to target the original ESP-IDF version configured by `.github/workflows/build.yml`.

```text
idf.py set-target esp32
idf.py build
```

The repository's CI workflow also performs the build on GitHub Actions.

## First installation

The existing `partitions.csv` is intentionally unchanged in this revision because OTA is deferred.

For a normal wired installation, flash the bootloader, partition table, and application produced by the build at the offsets reported by ESP-IDF/esptool.

## Configuration

Local AP settings, relay configuration, relay states, STA credentials, cloud credentials, brand name, and cached schedules are stored in NVS.

Cloud is optional. Local relay control and cached schedules do not require Internet access.

## Important hardware note

`RELAY_ACTIVE_LEVEL` is intentionally preserved from the supplied source. Verify the actual relay module's electrical active level before changing it; changing it blindly can invert relay behavior.

## OTA status

OTA is deliberately **out of scope for this revision**. Do not add `ota_0`, `ota_1`, or `otadata` manually to the supplied partition table. A later OTA revision should be designed as a coordinated bootloader/partition/firmware/backend feature with rollback and signed-image verification.
