#include "cloud_client.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "esp_log.h"
#include "esp_task_wdt.h"
#include "cJSON.h"
#include "nvs.h"
#include "nvs_flash.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#define TAG "CLOUD_CLIENT"
#define POLL_SECONDS 5
#define MAX_BACKOFF_SECONDS 60
#define HTTP_TIMEOUT_MS 8000
#define RESPONSE_MAX 12288
#define SCHEDULE_MAX CLOUD_SCHEDULE_MAX
#define NVS_NS "home_cfg"
#define NVS_SCHEDULES "sched22"
#define NVS_SCHEDULE_COUNT "sched_n2"
#define NVS_SCHEDULE_VERSION "sched_v2"
#define NVS_LEGACY_SCHEDULES "sched21"
#define NVS_LEGACY_COUNT "sched_n"

#define COMMAND_ACK_MAX 64

typedef cloud_schedule_t schedule_t;

typedef struct {
    bool enabled;
    int id;
    int relay;
    int hour;
    int minute;
    int action;
    int days;
    int duration_minutes;
} legacy_schedule_t;

typedef struct {
    char *buf;
    size_t cap;
    size_t used;
    bool truncated;
} response_capture_t;

static cloud_client_config_t g_cfg;
static volatile bool g_task_started = false;
static volatile bool g_online = false;
static volatile bool g_user_offline = false;
static TaskHandle_t g_task_handle = NULL;
static SemaphoreHandle_t cloud_mutex = NULL;
static SemaphoreHandle_t config_mutex = NULL;
static schedule_t schedules[SCHEDULE_MAX];
static size_t schedule_count = 0;
static uint32_t pending_ack_ids[COMMAND_ACK_MAX];
static size_t pending_ack_count = 0;

static SemaphoreHandle_t storage_lock_handle(void)
{
    return (SemaphoreHandle_t)g_cfg.storage_lock;
}

static bool valid_url(const char *u)
{
    return u && strncmp(u, "https://", 8) == 0;
}

static void copy_config(cloud_client_config_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!config_mutex) return;
    xSemaphoreTake(config_mutex, portMAX_DELAY);
    memcpy(out, &g_cfg, sizeof(*out));
    xSemaphoreGive(config_mutex);
}

static esp_err_t http_write_cb(esp_http_client_event_t *evt)
{
    if (!evt || !evt->user_data) return ESP_OK;
    response_capture_t *cap = (response_capture_t *)evt->user_data;
    if (evt->event_id == HTTP_EVENT_ON_DATA && evt->data_len > 0 && cap->buf && cap->cap > 0) {
        size_t room = cap->cap - 1 - cap->used;
        size_t n = evt->data_len < room ? (size_t)evt->data_len : room;
        if (n > 0) {
            memcpy(cap->buf + cap->used, evt->data, n);
            cap->used += n;
            cap->buf[cap->used] = '\0';
            if (n < (size_t)evt->data_len) cap->truncated = true;
        } else if (evt->data_len > 0) {
            cap->truncated = true;
        }
    }
    return ESP_OK;
}

static bool cloud_post(const cloud_client_config_t *cfg, const char *path,
                       const char *body, char *response, size_t response_sz)
{
    if (!cfg || !cfg->base_url[0] || !cfg->device_id[0] || !cfg->device_token[0] || !valid_url(cfg->base_url))
        return false;

    char url[320];
    size_t base_len = strlen(cfg->base_url);
    bool base_slash = base_len > 0 && cfg->base_url[base_len - 1] == '/';
    bool path_slash = path[0] == '/';
    int n;
    if (base_slash && path_slash) {
        n = snprintf(url, sizeof(url), "%.*s%s", (int)(base_len - 1), cfg->base_url, path);
    } else if (!base_slash && !path_slash) {
        n = snprintf(url, sizeof(url), "%s/%s", cfg->base_url, path);
    } else {
        n = snprintf(url, sizeof(url), "%s%s", cfg->base_url, path);
    }
    if (n < 0 || (size_t)n >= sizeof(url)) return false;

    if (response && response_sz) response[0] = '\0';
    response_capture_t cap = {.buf = response, .cap = response_sz, .used = 0, .truncated = false};
    esp_http_client_config_t http_cfg = {
        .url = url,
        .method = HTTP_METHOD_POST,
        .timeout_ms = HTTP_TIMEOUT_MS,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .user_data = &cap,
        .event_handler = http_write_cb,
        .keep_alive_enable = true,
        .disable_auto_redirect = true,
    };

    esp_http_client_handle_t h = esp_http_client_init(&http_cfg);
    if (!h) return false;

    char auth[160];
    int auth_len = snprintf(auth, sizeof(auth), "Bearer %s", cfg->device_token);
    bool ok = false;
    if (auth_len > 0 && (size_t)auth_len < sizeof(auth) &&
        esp_http_client_set_header(h, "Content-Type", "application/json") == ESP_OK &&
        esp_http_client_set_header(h, "Authorization", auth) == ESP_OK &&
        esp_http_client_set_post_field(h, body ? body : "{}", body ? strlen(body) : 2) == ESP_OK) {
        esp_err_t err = esp_http_client_perform(h);
        int code = (err == ESP_OK) ? esp_http_client_get_status_code(h) : 0;
        ok = (err == ESP_OK && code >= 200 && code < 300 && !cap.truncated);
        if (!ok) {
            ESP_LOGW(TAG, "Cloud request failed: err=%s http=%d", esp_err_to_name(err), code);
        }
    }

    esp_http_client_cleanup(h);
    return ok;
}

static bool save_schedules_locked(void)
{
    SemaphoreHandle_t storage = storage_lock_handle();
    if (storage) xSemaphoreTake(storage, portMAX_DELAY);

    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NS, NVS_READWRITE, &h);
    if (err != ESP_OK) {
        if (storage) xSemaphoreGive(storage);
        ESP_LOGE(TAG, "NVS open for schedules failed: %s", esp_err_to_name(err));
        return false;
    }

    const size_t blob_size = schedule_count * sizeof(schedule_t);
    if (blob_size == 0) {
        err = nvs_erase_key(h, NVS_SCHEDULES);
        if (err == ESP_ERR_NVS_NOT_FOUND) err = ESP_OK;
    } else {
        err = nvs_set_blob(h, NVS_SCHEDULES, schedules, blob_size);
    }
    if (err == ESP_OK) err = nvs_set_u8(h, NVS_SCHEDULE_COUNT, (uint8_t)schedule_count);
    if (err == ESP_OK) err = nvs_set_u8(h, NVS_SCHEDULE_VERSION, 2);
    if (err == ESP_OK) err = nvs_commit(h);
    nvs_close(h);

    if (storage) xSemaphoreGive(storage);
    if (err != ESP_OK) ESP_LOGE(TAG, "NVS schedule save failed: %s", esp_err_to_name(err));
    return err == ESP_OK;
}

static void load_schedules(void)
{
    memset(schedules, 0, sizeof(schedules));
    schedule_count = 0;

    nvs_handle_t h;
    if (nvs_open(NVS_NS, NVS_READONLY, &h) != ESP_OK) return;

    uint8_t version = 0, n = 0;
    esp_err_t verr = nvs_get_u8(h, NVS_SCHEDULE_VERSION, &version);
    esp_err_t nerr = nvs_get_u8(h, NVS_SCHEDULE_COUNT, &n);

    if (verr == ESP_OK && version == 2 && nerr == ESP_OK && n <= SCHEDULE_MAX) {
        if (n == 0) {
            nvs_close(h);
            return;
        }
        size_t sz = (size_t)n * sizeof(schedule_t);
        if (nvs_get_blob(h, NVS_SCHEDULES, schedules, &sz) == ESP_OK && sz == (size_t)n * sizeof(schedule_t)) {
            schedule_count = n;
            nvs_close(h);
            return;
        }
        memset(schedules, 0, sizeof(schedules));
        schedule_count = 0;
        ESP_LOGW(TAG, "Schedule cache invalid; ignoring it");
    }

    uint8_t old_n = 0;
    if (nvs_get_u8(h, NVS_LEGACY_COUNT, &old_n) == ESP_OK && old_n > 0 && old_n <= SCHEDULE_MAX) {
        legacy_schedule_t legacy[SCHEDULE_MAX];
        memset(legacy, 0, sizeof(legacy));
        size_t sz = (size_t)old_n * sizeof(legacy_schedule_t);
        if (nvs_get_blob(h, NVS_LEGACY_SCHEDULES, legacy, &sz) == ESP_OK && sz == (size_t)old_n * sizeof(legacy_schedule_t)) {
            size_t converted = 0;
            for (size_t i = 0; i < old_n && converted < SCHEDULE_MAX; ++i) {
                if (legacy[i].relay < 1 || legacy[i].relay > 10 ||
                    legacy[i].hour < 0 || legacy[i].hour > 23 ||
                    legacy[i].minute < 0 || legacy[i].minute > 59 ||
                    (legacy[i].action != 0 && legacy[i].action != 1) ||
                    legacy[i].days < 1 || legacy[i].days > 127 ||
                    legacy[i].duration_minutes < 0 || legacy[i].duration_minutes > 1439) continue;
                schedules[converted].enabled = legacy[i].enabled ? 1 : 0;
                schedules[converted].id = (uint8_t)converted;
                schedules[converted].relay = (uint8_t)legacy[i].relay;
                schedules[converted].hour = (uint8_t)legacy[i].hour;
                schedules[converted].minute = (uint8_t)legacy[i].minute;
                schedules[converted].action = (uint8_t)legacy[i].action;
                schedules[converted].days = (uint8_t)legacy[i].days;
                schedules[converted].duration_minutes = (uint16_t)legacy[i].duration_minutes;
                converted++;
            }
            schedule_count = converted;
            nvs_close(h);
            if (cloud_mutex) {
                xSemaphoreTake(cloud_mutex, portMAX_DELAY);
                bool ok = save_schedules_locked();
                xSemaphoreGive(cloud_mutex);
                if (!ok) ESP_LOGE(TAG, "Legacy schedule migration could not be saved");
            }
            ESP_LOGI(TAG, "Migrated %u legacy schedules", (unsigned)converted);
            return;
        }
    }
    nvs_close(h);
}

static void add_pending_ack(uint32_t id)
{
    if (!id || pending_ack_count >= COMMAND_ACK_MAX) return;
    for (size_t i = 0; i < pending_ack_count; ++i) if (pending_ack_ids[i] == id) return;
    pending_ack_ids[pending_ack_count++] = id;
}

static bool schedules_equal(const schedule_t *a, size_t ac, const schedule_t *b, size_t bc)
{
    return ac == bc && (ac == 0 || memcmp(a, b, ac * sizeof(schedule_t)) == 0);
}

static void parse_response(const char *json)
{
    cJSON *root = cJSON_Parse(json);
    if (!root) return;

    cJSON *cmds = cJSON_GetObjectItemCaseSensitive(root, "commands");
    if (cJSON_IsArray(cmds) && g_cfg.command_cb) {
        cJSON *c = NULL;
        cJSON_ArrayForEach(c, cmds) {
            cJSON *idv = cJSON_GetObjectItemCaseSensitive(c, "id");
            cJSON *rv = cJSON_GetObjectItemCaseSensitive(c, "relay");
            cJSON *sv = cJSON_GetObjectItemCaseSensitive(c, "state");
            if (!cJSON_IsNumber(idv) || !cJSON_IsNumber(rv) || !cJSON_IsNumber(sv)) continue;
            int relay = rv->valueint;
            int state = sv->valueint;
            if (relay >= 1 && relay <= 10 && (state == 0 || state == 1)) {
                g_cfg.command_cb(relay - 1, state, g_cfg.ctx);
                add_pending_ack((uint32_t)idv->valuedouble);
            }
        }
    }

    cJSON *sched = cJSON_GetObjectItemCaseSensitive(root, "schedules");
    if (cJSON_IsArray(sched)) {
        schedule_t tmp[SCHEDULE_MAX];
        memset(tmp, 0, sizeof(tmp));
        size_t n = 0;
        cJSON *x = NULL;
        cJSON_ArrayForEach(x, sched) {
            if (n >= SCHEDULE_MAX || !cJSON_IsObject(x)) break;
            cJSON *v;
            tmp[n].enabled = (v=cJSON_GetObjectItemCaseSensitive(x,"enabled")) ? cJSON_IsTrue(v) : false;
            tmp[n].id = (v=cJSON_GetObjectItemCaseSensitive(x,"id")) ? v->valueint : (int)n;
            tmp[n].relay = (v=cJSON_GetObjectItemCaseSensitive(x,"relay")) ? v->valueint : 0;
            tmp[n].hour = (v=cJSON_GetObjectItemCaseSensitive(x,"hour")) ? v->valueint : -1;
            tmp[n].minute = (v=cJSON_GetObjectItemCaseSensitive(x,"minute")) ? v->valueint : -1;
            tmp[n].action = (v=cJSON_GetObjectItemCaseSensitive(x,"action")) ? v->valueint : 0;
            tmp[n].days = (v=cJSON_GetObjectItemCaseSensitive(x,"days")) ? v->valueint : 127;
            tmp[n].duration_minutes = (v=cJSON_GetObjectItemCaseSensitive(x,"durationMinutes")) ? v->valueint : 0;
            if (tmp[n].relay >= 1 && tmp[n].relay <= 10 && tmp[n].hour < 24 &&
                tmp[n].minute < 60 && (tmp[n].action == 0 || tmp[n].action == 1) &&
                tmp[n].days >= 1 && tmp[n].days <= 127 && tmp[n].duration_minutes <= 1439) n++;
        }
        xSemaphoreTake(cloud_mutex, portMAX_DELAY);
        bool changed = !schedules_equal(schedules, schedule_count, tmp, n);
        if (changed) {
            memcpy(schedules, tmp, n * sizeof(schedule_t));
            if (n < SCHEDULE_MAX) memset(&schedules[n], 0, (SCHEDULE_MAX - n) * sizeof(schedule_t));
            schedule_count = n;
            if (!save_schedules_locked()) ESP_LOGE(TAG, "Cloud schedule cache could not be persisted");
        }
        xSemaphoreGive(cloud_mutex);
    }
    cJSON_Delete(root);
}

static size_t build_poll_body(const cloud_client_config_t *cfg, char *out, size_t out_sz,
                              int states[5], bool enabled[5])
{
    cJSON *root = cJSON_CreateObject();
    if (!root) return 0;
    cJSON_AddStringToObject(root, "deviceId", cfg->device_id);
    cJSON *sa = cJSON_CreateArray(), *ea = cJSON_CreateArray(), *acks = cJSON_CreateArray();
    if (!sa || !ea || !acks) { cJSON_Delete(root); return 0; }
    for (int i = 0; i < 5; ++i) {
        cJSON_AddItemToArray(sa, cJSON_CreateNumber(states[i] ? 1 : 0));
        cJSON_AddItemToArray(ea, cJSON_CreateBool(enabled[i]));
    }
    for (size_t i = 0; i < pending_ack_count; ++i) cJSON_AddItemToArray(acks, cJSON_CreateNumber((double)pending_ack_ids[i]));
    cJSON_AddItemToObject(root, "states", sa);
    cJSON_AddItemToObject(root, "enabled", ea);
    cJSON_AddItemToObject(root, "ackIds", acks);
    char *printed = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!printed) return 0;
    size_t n = strlen(printed);
    if (n >= out_sz) { free(printed); return 0; }
    memcpy(out, printed, n + 1);
    free(printed);
    return n;
}

static void clear_acks_if_request_succeeded(void)
{
    pending_ack_count = 0;
}

static void cloud_task(void *arg)
{
    (void)arg;
    uint32_t backoff = 0;
    esp_task_wdt_add(NULL);

    while (1) {
        if (g_user_offline) {
            g_online = false;
            vTaskDelay(pdMS_TO_TICKS(1000));
            esp_task_wdt_reset();
            continue;
        }

        cloud_client_config_t cfg;
        copy_config(&cfg);
        if (!cfg.base_url[0] || !cfg.device_id[0] || !cfg.device_token[0]) {
            g_online = false;
            vTaskDelay(pdMS_TO_TICKS(2000));
            esp_task_wdt_reset();
            continue;
        }

        int states[5] = {0};
        bool enabled[5] = {0};
        if (cfg.snapshot_cb) cfg.snapshot_cb(states, enabled, cfg.ctx);

        char body[4096];
        char response[RESPONSE_MAX];
        bool body_ok = build_poll_body(&cfg, body, sizeof(body), states, enabled) > 0;
        bool ok = body_ok && cloud_post(&cfg, "/api/device/poll", body, response, sizeof(response));
        g_online = ok;

        if (ok) {
            parse_response(response);
            clear_acks_if_request_succeeded();
            backoff = 0;
        } else {
            if (backoff == 0) backoff = 5;
            else if (backoff < MAX_BACKOFF_SECONDS) backoff = (backoff * 2 > MAX_BACKOFF_SECONDS) ? MAX_BACKOFF_SECONDS : backoff * 2;
        }

        uint32_t delay = ok ? POLL_SECONDS : backoff;
        for (uint32_t s = 0; s < delay; ++s) {
            if (g_task_handle && ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(1000)) > 0) break;
            esp_task_wdt_reset();
            if (g_user_offline) break;
        }
    }
}

size_t cloud_client_get_schedules(cloud_schedule_t *out, size_t max_count)
{
    if (!out || max_count == 0 || !cloud_mutex) return 0;
    xSemaphoreTake(cloud_mutex, portMAX_DELAY);
    size_t n = schedule_count < max_count ? schedule_count : max_count;
    memcpy(out, schedules, n * sizeof(schedule_t));
    xSemaphoreGive(cloud_mutex);
    return n;
}

bool cloud_client_replace_schedules(const cloud_schedule_t *items, size_t count)
{
    if (count > CLOUD_SCHEDULE_MAX || (count > 0 && !items) || !cloud_mutex) return false;
    schedule_t tmp[CLOUD_SCHEDULE_MAX];
    memset(tmp, 0, sizeof(tmp));
    for (size_t i = 0; i < count; ++i) {
        if (items[i].relay < 1 || items[i].relay > 10 || items[i].hour > 23 || items[i].minute > 59 ||
            (items[i].action != 0 && items[i].action != 1) || items[i].days < 1 || items[i].days > 127 ||
            items[i].duration_minutes > 1439) return false;
        tmp[i] = items[i];
        tmp[i].id = (uint8_t)i;
    }
    xSemaphoreTake(cloud_mutex, portMAX_DELAY);
    schedule_t old[SCHEDULE_MAX];
    size_t old_count = schedule_count;
    memcpy(old, schedules, sizeof(old));
    memcpy(schedules, tmp, count * sizeof(schedule_t));
    if (count < SCHEDULE_MAX) memset(&schedules[count], 0, (SCHEDULE_MAX - count) * sizeof(schedule_t));
    schedule_count = count;
    bool saved = save_schedules_locked();
    if (!saved) {
        memcpy(schedules, old, sizeof(old));
        schedule_count = old_count;
    }
    xSemaphoreGive(cloud_mutex);
    return saved;
}

void cloud_client_init(const cloud_client_config_t *cfg)
{
    if (config_mutex) return;
    config_mutex = xSemaphoreCreateMutex();
    cloud_mutex = xSemaphoreCreateMutex();
    if (!config_mutex || !cloud_mutex) {
        ESP_LOGE(TAG, "Cloud mutex allocation failed");
        return;
    }
    memset(&g_cfg, 0, sizeof(g_cfg));
    if (cfg) memcpy(&g_cfg, cfg, sizeof(g_cfg));
    load_schedules();

    if (!g_cfg.base_url[0] || !g_cfg.device_id[0] || !g_cfg.device_token[0]) {
        ESP_LOGI(TAG, "Cloud not configured; local-only mode");
        return;
    }
    if (xTaskCreate(cloud_task, "cloud_client", 7168, NULL, 3, &g_task_handle) == pdPASS) {
        g_task_started = true;
    } else {
        ESP_LOGE(TAG, "Cloud task creation failed");
    }
}

void cloud_client_set_credentials(const char *base_url, const char *device_id, const char *device_token)
{
    if (!config_mutex) return;
    xSemaphoreTake(config_mutex, portMAX_DELAY);
    if (base_url) strlcpy(g_cfg.base_url, base_url, sizeof(g_cfg.base_url));
    if (device_id) strlcpy(g_cfg.device_id, device_id, sizeof(g_cfg.device_id));
    if (device_token) strlcpy(g_cfg.device_token, device_token, sizeof(g_cfg.device_token));
    bool have_creds = g_cfg.base_url[0] && g_cfg.device_id[0] && g_cfg.device_token[0];
    xSemaphoreGive(config_mutex);

    g_online = false;
    if (have_creds && !g_task_started) {
        if (xTaskCreate(cloud_task, "cloud_client", 7168, NULL, 3, &g_task_handle) == pdPASS) {
            g_task_started = true;
        } else {
            ESP_LOGE(TAG, "Cloud task creation failed");
        }
    }
    if (g_task_handle) xTaskNotifyGive(g_task_handle);
}

bool cloud_client_is_online(void) { return g_online; }

void cloud_client_set_offline(bool offline)
{
    g_user_offline = offline;
    if (offline) g_online = false;
    if (g_task_handle) xTaskNotifyGive(g_task_handle);
}

bool cloud_client_is_user_offline(void) { return g_user_offline; }
