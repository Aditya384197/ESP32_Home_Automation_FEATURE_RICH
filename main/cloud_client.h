#pragma once
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CLOUD_SCHEDULE_MAX 64

typedef void (*cloud_command_cb_t)(int relay, int state, void *ctx);
typedef void (*cloud_snapshot_cb_t)(int *states, bool *enabled, void *ctx);

typedef struct {
    char base_url[192];
    char device_id[64];
    char device_token[128];
    cloud_command_cb_t command_cb;
    cloud_snapshot_cb_t snapshot_cb;
    void *ctx;
    void *storage_lock;
} cloud_client_config_t;

typedef struct {
    uint16_t duration_minutes;
    uint8_t enabled;
    uint8_t id;
    uint8_t relay;
    uint8_t hour;
    uint8_t minute;
    uint8_t action;
    uint8_t days;
} cloud_schedule_t;

void cloud_client_init(const cloud_client_config_t *cfg);
bool cloud_client_is_online(void);
size_t cloud_client_get_schedules(cloud_schedule_t *out, size_t max_count);
bool cloud_client_replace_schedules(const cloud_schedule_t *items, size_t count);
void cloud_client_set_offline(bool offline);
bool cloud_client_is_user_offline(void);
void cloud_client_set_credentials(const char *base_url, const char *device_id, const char *device_token);
