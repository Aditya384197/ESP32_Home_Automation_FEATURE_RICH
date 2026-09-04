#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"

#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_err.h"
#include "esp_netif.h"
#include "esp_http_server.h"
#include "esp_task_wdt.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "driver/gpio.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "esp_sntp.h"
#include "esp_timer.h"
#include "mdns.h"
#include "mqtt_client.h"
#include "cJSON.h"
#include "cloud_client.h"

#define TAG "SMART_HOME"

#define RELAY1_GPIO             16
#define RELAY2_GPIO             17
#define RELAY3_GPIO             18
#define RELAY4_GPIO             19
#define RELAY5_GPIO             21
#define RELAY6_GPIO             22
#define RELAY7_GPIO             23

#define RELAY_COUNT             7

#define SWITCH1_GPIO            32
#define SWITCH2_GPIO            33
#define SWITCH3_GPIO            25
#define SWITCH4_GPIO            26
#define SWITCH5_GPIO            27
#define SWITCH_COUNT            5
#define SWITCH_ACTIVE_LEVEL     0
#define SWITCH_DEBOUNCE_SAMPLES 3
#define SWITCH_POLL_MS          20

#define RELAY_ACTIVE_LEVEL      1

#define DEFAULT_AP_SSID         "ESP32-SMART-HOME"
#define DEFAULT_AP_PASSWORD     "ChangeMe123"
#define DEFAULT_AP_CHANNEL      6
#define AP_MAX_CONNECTIONS      4

#define AP_IP_ADDR              "192.168.4.1"
#define AP_GW_ADDR              "192.168.4.1"
#define AP_NETMASK              "255.255.255.0"

#define NVS_NAMESPACE           "home_cfg"
#define NVS_KEY_RELAY_STATES    "relay"
#define NVS_KEY_RELAY_ENABLED   "renable"
#define NVS_KEY_RELAY_NAMES     "rnames"
#define NVS_KEY_AP_SSID         "ap_ssid"
#define NVS_KEY_AP_PASS         "ap_pass"
#define NVS_KEY_STA_SSID        "sta_ssid"
#define NVS_KEY_STA_PASS        "sta_pass"
#define NVS_KEY_CLOUD_URL       "cloud_url"
#define NVS_KEY_DEVICE_ID       "device_id"
#define NVS_KEY_DEVICE_TOKEN    "device_token"
#define NVS_KEY_BRAND_NAME      "brand_name"
#define NVS_KEY_ROOM_NAMES      "room_names"
#define NVS_KEY_MQTT_HOST       "mqtt_host"
#define NVS_KEY_MQTT_PORT       "mqtt_port"
#define NVS_KEY_MQTT_USER       "mqtt_user"
#define NVS_KEY_MQTT_PASS       "mqtt_pass"
#define NVS_KEY_MQTT_ENABLED    "mqtt_en"

#define WATCHDOG_TIMEOUT_MS     10000
#define DNS_PORT                53
#define DNS_STACK_SIZE          3072
#define DNS_RX_SIZE             512

#define MAX_AP_SSID_LEN         32
#define MAX_AP_PASS_LEN         63
#define MAX_RELAY_NAME_LEN      31
#define MAX_CLOUD_URL_LEN       191
#define MAX_DEVICE_ID_LEN       63
#define MAX_DEVICE_TOKEN_LEN    127
#define MAX_BRAND_LEN            40
#define MAX_ROOM_NAME_LEN        24
#define MAX_MQTT_HOST_LEN       127
#define MAX_MQTT_USER_LEN       63
#define MAX_MQTT_PASS_LEN       63

static int relay_state[RELAY_COUNT] = {0};
static bool relay_enabled[RELAY_COUNT] = {true, true, true, true, true, true, true};
static char relay_name[RELAY_COUNT][MAX_RELAY_NAME_LEN + 1] = {
    "Relay 1", "Relay 2", "Relay 3", "Relay 4", "Relay 5", "Relay 6", "Relay 7"
};
static char room_name[3][MAX_ROOM_NAME_LEN + 1] = {"Room 1", "Room 2", "Room 3"};

static SemaphoreHandle_t relay_mutex;
static SemaphoreHandle_t storage_mutex;

static char ap_ssid[MAX_AP_SSID_LEN + 1] = DEFAULT_AP_SSID;
static char ap_password[MAX_AP_PASS_LEN + 1] = DEFAULT_AP_PASSWORD;
static char sta_ssid[MAX_AP_SSID_LEN + 1] = "";
static char sta_password[MAX_AP_PASS_LEN + 1] = "";
static char cloud_url[MAX_CLOUD_URL_LEN + 1] = "";
static char device_id[MAX_DEVICE_ID_LEN + 1] = "";
static char device_token[MAX_DEVICE_TOKEN_LEN + 1] = "";
static char brand_name[MAX_BRAND_LEN + 1] = "Smart Home";
static volatile bool sta_connected = false;
static volatile uint8_t sta_retry_count = 0;
static volatile bool user_offline_mode = false;
static char sta_ip[16] = {0};
static char mdns_host[MAX_BRAND_LEN + 8] = "smarthome";

static char mqtt_host[MAX_MQTT_HOST_LEN + 1] = "";
static uint16_t mqtt_port = 1883;
static char mqtt_user[MAX_MQTT_USER_LEN + 1] = "";
static char mqtt_pass[MAX_MQTT_PASS_LEN + 1] = "";
static bool mqtt_enabled = false;
static esp_mqtt_client_handle_t mqtt_client = NULL;
static volatile bool mqtt_connected = false;
static char mqtt_base_topic[96] = "";

static TaskHandle_t dns_task_handle = NULL;
static TaskHandle_t switch_task_handle = NULL;
static TaskHandle_t relay_save_task_handle = NULL;
static httpd_handle_t http_server = NULL;
static TaskHandle_t schedule_task_handle = NULL;
static TaskHandle_t sta_reconnect_task_handle = NULL;
static bool schedule_was_active[RELAY_COUNT] = {false, false, false, false, false};
static bool schedule_override[RELAY_COUNT] = {false, false, false, false, false};
static int schedule_revert_state[RELAY_COUNT] = {0, 0, 0, 0, 0};
static volatile bool g_time_synced = false;

static void schedule_note_manual_change(int index)
{
    if (index < 0 || index >= RELAY_COUNT) return;
    if (schedule_was_active[index]) schedule_override[index] = true;
}

static const char *HTML_PAGE =
"<!doctype html>\n"
"<html lang=\"en\">\n"
"<head>\n"
"<meta charset=\"utf-8\">\n"
"<meta name=\"viewport\" content=\"width=device-width,initial-scale=1,viewport-fit=cover\">\n"
"<meta name=\"theme-color\" content=\"#111\">\n"
"<title>ESP32 Smart Home</title>\n"
"<style>\n"
":root{color-scheme:light;--bg:#f5f6f7;--card:#fff;--text:#121315;--muted:#74777d;--line:#e1e4e8;--panel:#eceff2;--accent:#111;--on:#111;--danger:#a33b3b;--press:rgba(0,0,0,.055);--shadow:0 8px 24px rgba(0,0,0,.08)}\n"
":root[data-theme=\"dark\"]{color-scheme:dark;--bg:#0d0e10;--card:#191b1f;--text:#f4f5f6;--muted:#9da1a8;--line:#2a2e33;--panel:#15171a;--accent:#f4f5f6;--on:#f4f5f6;--danger:#db8c8c;--press:rgba(255,255,255,.07);--shadow:0 10px 28px rgba(0,0,0,.28)}\n"
"*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}html,body{margin:0;height:100%;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--text)}body{overflow:hidden;overscroll-behavior-y:none}.app{height:100dvh;display:flex;flex-direction:column}\n"
".top{flex:none;background:var(--panel);border-bottom:1px solid var(--line);padding:calc(env(safe-area-inset-top,0px) + 12px) 14px 12px;z-index:2}.topbar{max-width:720px;margin:auto;display:grid;grid-template-columns:42px 1fr 42px;gap:10px;align-items:center}.logo-box{width:40px;height:40px;border-radius:13px;background:var(--card);border:1px solid var(--line);display:grid;place-items:center;box-shadow:0 4px 10px rgba(0,0,0,.05)}.logo-box svg{width:22px;height:22px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.brand{text-align:center;min-width:0}.brand h1{margin:0;font-size:23px;line-height:1.1;font-weight:750;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.settings-btn,.icon-btn{width:42px;height:42px;border:1px solid var(--line);border-radius:13px;background:var(--card);color:var(--text);display:grid;place-items:center;cursor:pointer}.settings-btn:active,.icon-btn:active,.room-head:active,.relay-card:active,.setting-item:active{transform:scale(.985)}.settings-btn svg,.icon-btn svg{width:20px;height:20px;fill:none;stroke:currentColor;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}\n"
"#controls{flex:1;overflow:auto;padding:12px;display:flex;flex-direction:column;gap:12px;max-width:720px;width:100%;margin:auto;scrollbar-width:none}#controls::-webkit-scrollbar{display:none}.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:12px}.main-relay{min-width:0}.relay-card,.room-card{background:var(--card);border:1px solid var(--line);border-radius:16px;box-shadow:var(--shadow)}.relay-card{min-height:76px;padding:13px 14px;display:flex;align-items:center;gap:11px;justify-content:space-between;cursor:pointer}.relay-meta{min-width:0}.relay-name{font-size:16px;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.relay-state{font-size:11px;font-weight:700;letter-spacing:.6px;color:var(--muted);margin-top:3px}.switch{position:relative;width:52px;height:30px;flex:none}.switch input{opacity:0;width:0;height:0}.slider{position:absolute;inset:0;background:var(--line);border-radius:99px;transition:.18s}.slider:before{content:\"\";position:absolute;width:24px;height:24px;left:3px;top:3px;background:var(--card);border-radius:50%;box-shadow:0 1px 4px rgba(0,0,0,.22);transition:transform .2s cubic-bezier(.34,1.2,.64,1)}input:checked+.slider{background:var(--on)}input:checked+.slider:before{transform:translateX(22px);background:var(--bg)}\n"
".room-card{overflow:hidden}.room-head{width:100%;border:0;background:transparent;color:var(--text);padding:15px 15px;display:flex;align-items:center;gap:11px;text-align:left;cursor:pointer}.room-badge{width:38px;height:38px;border-radius:12px;background:var(--panel);border:1px solid var(--line);display:grid;place-items:center;font-weight:800;font-size:14px;flex:none}.room-title-wrap{min-width:0;flex:1}.room-title{font-size:16px;font-weight:750}.room-summary{font-size:11px;color:var(--muted);margin-top:3px}.room-chevron{width:18px;height:18px;transition:transform .24s ease;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.room-card.open .room-chevron{transform:rotate(180deg)}.room-body{display:grid;grid-template-rows:0fr;transition:grid-template-rows .28s cubic-bezier(.22,.8,.2,1);border-top:0}.room-card.open .room-body{grid-template-rows:1fr;border-top:1px solid var(--line)}.room-body-inner{overflow:hidden}.room-controls{padding:10px;display:grid;grid-template-columns:1fr;gap:8px}.compact-relay{min-height:58px;padding:10px 11px;border-radius:13px;box-shadow:none;background:var(--panel)}.compact-relay .relay-name{font-size:14px}.compact-relay .relay-state{font-size:10px}.empty{padding:12px;color:var(--muted);font-size:12px}\n"
".drawer-backdrop{position:fixed;inset:0;background:rgba(16,20,28,.36);opacity:0;pointer-events:none;transition:opacity .25s;z-index:20}.drawer-backdrop.open{opacity:1;pointer-events:auto}.settings-drawer{position:fixed;inset:0 0 0 auto;width:min(680px,100%);background:var(--bg);transform:translateX(104%);transition:transform .3s cubic-bezier(.22,.8,.2,1);z-index:30;overflow:auto;box-shadow:-14px 0 34px rgba(0,0,0,.18)}.settings-drawer.open{transform:translateX(0)}.drawer-inner{min-height:100%;padding:calc(env(safe-area-inset-top,0px) + 14px) 14px 28px}.drawer-top{display:grid;grid-template-columns:1fr 42px;gap:12px;align-items:center;padding:4px 2px 16px}.drawer-title{font-size:24px;font-weight:800}.drawer-status{margin-top:7px;display:flex;align-items:center;gap:9px}.conn-toggle{width:38px;height:38px;border:1px solid var(--line);border-radius:12px;background:var(--card);color:var(--muted);display:grid;place-items:center;cursor:pointer}.conn-toggle svg{width:19px;height:19px;fill:none;stroke:currentColor;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}.conn-toggle.online{color:var(--text)}.conn-toggle.trying{color:var(--muted);animation:pulse 1.2s ease-in-out infinite}.conn-toggle.offline{color:var(--danger)}@keyframes pulse{50%{opacity:.45;transform:rotate(18deg)}}\n"
".setting-list{background:var(--card);border:1px solid var(--line);border-radius:16px;box-shadow:var(--shadow);overflow:hidden}.setting-item{display:flex;align-items:center;gap:13px;padding:14px 13px;border-top:1px solid var(--line);cursor:pointer}.setting-item:first-child{border-top:0}.setting-icon{width:40px;height:40px;border-radius:12px;background:var(--panel);border:1px solid var(--line);display:grid;place-items:center;font-size:17px;flex:none}.setting-title{font-size:15px;font-weight:700}.chevron{margin-left:auto;color:var(--muted);font-size:22px;line-height:1}.subpage{display:none}.subpage.active{display:block;animation:pageIn .25s ease}@keyframes pageIn{from{opacity:0;transform:translateX(10px)}to{opacity:1;transform:none}}.page-head{display:grid;grid-template-columns:42px 1fr;gap:10px;align-items:center;margin-bottom:15px}.page-title{font-size:21px;font-weight:800}.page-sub{font-size:12px;color:var(--muted);margin-top:3px}.info-card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:15px;box-shadow:var(--shadow)}.field{display:block;font-size:12px;color:var(--muted);margin:11px 0 6px}.field:first-child{margin-top:0}input[type=text],input[type=password],input[type=number],select{width:100%;padding:11px 12px;border:1px solid var(--line);border-radius:11px;background:var(--bg);color:var(--text);font:inherit}.bar{display:flex;gap:8px;flex-wrap:wrap;margin-top:13px}button{border:1px solid var(--line);background:var(--card);color:var(--text);border-radius:11px;padding:10px 13px;font:inherit;cursor:pointer}button.primary{background:var(--on);border-color:var(--on);color:var(--bg)}button:disabled{opacity:.5}.msg{font-size:12px;color:var(--muted);margin-top:9px}.small{font-size:12px;color:var(--muted);line-height:1.45}.diag-row{display:flex;align-items:center;justify-content:space-between;padding:11px 0;border-top:1px solid var(--line);font-size:13px}.diag-row:first-child{border-top:0}.diag-row span{color:var(--muted)}.theme-seg{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-top:4px}.theme-seg button.active{background:var(--on);color:var(--bg);border-color:var(--on)}.relay-config-item{padding:14px 0;border-top:1px solid var(--line)}.relay-config-item:first-child{border-top:0}.relay-config-head{display:flex;align-items:center;justify-content:space-between;gap:12px}.relay-number{font-weight:750}.relay-gpio,.relay-switch-gpio{font-size:11px;color:var(--muted);margin-top:3px}.small-switch{position:relative;width:46px;height:26px;flex:none}.small-switch input{opacity:0;width:0;height:0}.small-slider{position:absolute;inset:0;background:var(--line);border-radius:99px}.small-slider:before{content:\"\";position:absolute;width:20px;height:20px;left:3px;top:3px;border-radius:50%;background:var(--card);transition:.14s;box-shadow:0 1px 3px rgba(0,0,0,.22)}.small-switch input:checked+.small-slider{background:var(--on)}.small-switch input:checked+.small-slider:before{transform:translateX(20px);background:var(--bg)}.room-name-grid{display:grid;grid-template-columns:1fr 1fr;gap:9px}.room-name-grid .full{grid-column:1/-1}.days{display:flex;flex-wrap:wrap;gap:5px;margin-top:8px}.days label{font-size:11px;display:flex;align-items:center;gap:3px}.schedule-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.time-row{display:flex;gap:7px;align-items:center}.time-row select{flex:1}.time-row .ampm{flex:.72}.time-sep{font-weight:800;color:var(--muted)}.time-readout{padding:11px;border:1px solid var(--line);border-radius:11px;color:var(--muted);background:var(--bg);font-size:12px}.list-row{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:13px 2px;border-top:1px solid var(--line);cursor:pointer}.list-row:first-child{border-top:0}.list-row strong{font-size:13px}.list-row-sub{display:block;color:var(--muted);font-size:11px;margin-top:3px}.row-disabled{opacity:.48}.back-row{margin-top:16px;text-align:center}.back-btn{min-width:180px}\n"
"@media(max-width:560px){.grid-2{gap:8px}.room-name-grid,.schedule-grid{grid-template-columns:1fr}.brand h1{font-size:21px}.relay-card{min-height:70px;padding:11px 12px}}\n"
"@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}\n"
"</style>\n"
"</head>\n"
"<body>\n"
"<div class=\"app\">\n"
"<header class=\"top\"><div class=\"topbar\">\n"
"<div class=\"logo-box\" aria-label=\"Logo\"><svg viewBox=\"0 0 24 24\"><path d=\"M3 10.5 12 3l9 7.5\"></path><path d=\"M5.5 9.5V21h13V9.5\"></path><path d=\"M9 21v-6h6v6\"></path></svg></div>\n"
"<div class=\"brand\"><h1 id=\"brandTitle\">Smart Home</h1></div>\n"
"<button class=\"settings-btn\" onclick=\"openSettings()\" aria-label=\"Settings\"><svg viewBox=\"0 0 24 24\"><path d=\"M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z\"></path><path d=\"m19 13 .1-.9-.1-.9 2-1.5-2-3.4-2.4 1a8 8 0 0 0-1.5-.9L14.8 3h-3.6l-.3 2.4c-.5.2-1 .5-1.5.9L7 6.3 5 9.7l2 1.5-.1.9.1.9-2 1.5L7 18l2.4-1c.5.4 1 .7 1.5.9l.3 2.4h3.6l.3-2.4c.5-.2 1-.5 1.5-.9l2.4 1 2-3.4-2-1.6Z\"></path></svg></button>\n"
"</div></header>\n"
"<section id=\"controls\"></section>\n"
"</div>\n"
"<div id=\"drawerBackdrop\" class=\"drawer-backdrop\" onclick=\"closeSettings()\"></div>\n"
"<aside id=\"settingsDrawer\" class=\"settings-drawer\" aria-hidden=\"true\"><div class=\"drawer-inner\">\n"
"<section id=\"settingsHome\" class=\"subpage active\">\n"
"<header class=\"drawer-top\"><div><div class=\"drawer-title\">Settings</div><div class=\"drawer-status\"><button id=\"connToggle\" class=\"conn-toggle\" onclick=\"toggleConnectivity()\" aria-label=\"Connectivity\"><svg id=\"connIcon\" viewBox=\"0 0 24 24\"></svg></button></div></div><button class=\"icon-btn\" onclick=\"closeSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button></header>\n"
"<div class=\"setting-list\">\n"
"<div class=\"setting-item\" onclick=\"openSubPage('schedulePage')\"><div class=\"setting-icon\">◷</div><div class=\"setting-title\">Schedule</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('appearancePage')\"><div class=\"setting-icon\">◐</div><div class=\"setting-title\">Appearance</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('brandPage')\"><div class=\"setting-icon\">✎</div><div class=\"setting-title\">Custom Logo / Name</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('relayPage')\"><div class=\"setting-icon\">▣</div><div class=\"setting-title\">Relay Configuration</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('internetPage')\"><div class=\"setting-icon\">◎</div><div class=\"setting-title\">Internet Connection</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('remotePage')\"><div class=\"setting-icon\">☁</div><div class=\"setting-title\">Remote Access</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('mqttPage')\"><div class=\"setting-icon\">⌁</div><div class=\"setting-title\">MQTT / Home Assistant</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('apPage')\"><div class=\"setting-icon\">≋</div><div class=\"setting-title\">AP Configuration</div><div class=\"chevron\">›</div></div>\n"
"<div class=\"setting-item\" onclick=\"openSubPage('diagPage')\"><div class=\"setting-icon\">◈</div><div class=\"setting-title\">Diagnostics</div><div class=\"chevron\">›</div></div>\n"
"</div></section>\n"
"<section id=\"appearancePage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Appearance</div></div><div class=\"info-card\"><div class=\"theme-seg\" id=\"themeSeg\"><button data-th=\"auto\" onclick=\"setTheme('auto')\">Auto</button><button data-th=\"light\" onclick=\"setTheme('light')\">Light</button><button data-th=\"dark\" onclick=\"setTheme('dark')\">Dark</button></div></div></section>\n"
"<section id=\"brandPage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Custom Logo / Name</div></div><div class=\"info-card\"><label class=\"field\">Main page name</label><input id=\"brandInput\" type=\"text\" maxlength=\"40\"><div class=\"bar\"><button class=\"primary\" onclick=\"saveBrand()\">Save</button></div><div id=\"brandMsg\" class=\"msg\"></div></div></section>\n"
"<section id=\"relayPage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Relay Configuration</div></div><div class=\"info-card\"><div class=\"room-name-grid\"><div><label class=\"field\">Room 1</label><input id=\"room1Name\" maxlength=\"24\"></div><div><label class=\"field\">Room 2</label><input id=\"room2Name\" maxlength=\"24\"></div><div class=\"full\"><label class=\"field\">Room 3</label><input id=\"room3Name\" maxlength=\"24\"></div></div><div id=\"relayConfigList\"></div><div class=\"bar\"><button class=\"primary\" onclick=\"saveRelayConfig()\">Save</button></div><div id=\"relaymsg\" class=\"msg\"></div></div></section>\n"
"<section id=\"internetPage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Internet Connection</div></div><div class=\"info-card\"><label class=\"field\">Home Wi-Fi SSID</label><input id=\"staSsid\" maxlength=\"32\"><label class=\"field\">Home Wi-Fi Password</label><input id=\"staPass\" type=\"password\" maxlength=\"63\"><div id=\"wifiStatus\" class=\"msg\">Not configured</div><div class=\"bar\"><button class=\"primary\" onclick=\"saveWifiSta()\">Connect Wi-Fi</button></div><div id=\"wifiMsg\" class=\"msg\"></div></div></section>\n"
"<section id=\"remotePage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Remote Access</div></div><div class=\"info-card\"><label class=\"field\">Cloud API URL</label><input id=\"cloudUrl\" maxlength=\"191\"><label class=\"field\">Device ID</label><input id=\"deviceId\" maxlength=\"63\"><label class=\"field\">Device Token</label><input id=\"deviceToken\" type=\"password\" maxlength=\"127\"><div id=\"cloudStatus\" class=\"msg\">Not configured</div><div class=\"bar\"><button class=\"primary\" onclick=\"saveCloud()\">Save</button></div><div id=\"cloudMsg\" class=\"msg\"></div></div></section>\n"
"<section id=\"mqttPage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">MQTT / Home Assistant</div></div><div class=\"info-card\"><label class=\"small\"><input id=\"mqttEnabled\" type=\"checkbox\"> Enable MQTT</label><label class=\"field\">Broker host or IP</label><input id=\"mqttHost\" maxlength=\"127\"><label class=\"field\">Broker port</label><input id=\"mqttPort\" type=\"number\" min=\"1\" max=\"65535\" value=\"1883\"><label class=\"field\">Username</label><input id=\"mqttUser\" maxlength=\"63\"><label class=\"field\">Password</label><input id=\"mqttPass\" type=\"password\" maxlength=\"63\"><div id=\"mqttStatus\" class=\"msg\">Not configured</div><div class=\"bar\"><button class=\"primary\" onclick=\"saveMqtt()\">Save</button></div><div id=\"mqttMsg\" class=\"msg\"></div></div></section>\n"
"<section id=\"apPage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">AP Configuration</div></div><div class=\"info-card\"><label class=\"field\">SSID</label><input id=\"ssid\" maxlength=\"32\"><label class=\"field\">Password</label><input id=\"pass\" type=\"password\" maxlength=\"63\"><div class=\"bar\"><button class=\"primary\" onclick=\"saveSettings()\">Save & Restart</button></div><div id=\"setmsg\" class=\"msg\"></div></div></section>\n"
"<section id=\"diagPage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Diagnostics</div></div><div class=\"info-card\"><div class=\"diag-row\"><span>mDNS address</span><strong id=\"diagMdns\">—</strong></div><div class=\"diag-row\"><span>Wi-Fi signal (RSSI)</span><strong id=\"diagRssi\">—</strong></div><div class=\"diag-row\"><span>Free heap</span><strong id=\"diagHeap\">—</strong></div><div class=\"diag-row\"><span>Lowest free heap</span><strong id=\"diagMinHeap\">—</strong></div><div class=\"diag-row\"><span>Uptime</span><strong id=\"diagUptime\">—</strong></div><div class=\"diag-row\"><span>Optional GPIO</span><strong id=\"diagOptionalGpio\">—</strong></div></div></section>\n"
"<section id=\"schedulePage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backToSettings()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Schedule</div></div><div class=\"info-card\"><div id=\"scheduleList\"></div></div><div class=\"bar\"><button onclick=\"addSchedule()\">＋ Add schedule</button></div><div id=\"scheduleMsg\" class=\"msg\"></div></section>\n"
"<section id=\"scheduleDetailPage\" class=\"subpage\"><div class=\"page-head\"><button class=\"icon-btn\" onclick=\"backFromScheduleDetail()\" aria-label=\"Back\"><svg viewBox=\"0 0 24 24\"><path d=\"m15 18-6-6 6-6\"></path></svg></button><div class=\"page-title\">Schedule</div></div><div class=\"info-card\"><div class=\"schedule-grid\"><div><label class=\"field\">Relay</label><select id=\"sdRelay\"></select></div><div><label class=\"field\">Action</label><select id=\"sdAction\" onchange=\"updateScheduleDetailEnd()\"></select></div></div><label class=\"field\">Start time</label><div class=\"time-row\"><select id=\"sdHour\" onchange=\"updateScheduleDetailEnd()\"></select><span class=\"time-sep\">:</span><select id=\"sdMinute\" onchange=\"updateScheduleDetailEnd()\"></select><select id=\"sdAmPm\" class=\"ampm\" onchange=\"updateScheduleDetailEnd()\"></select></div><div class=\"schedule-grid\"><div><label class=\"field\">Duration hours</label><input id=\"sdDurH\" type=\"number\" min=\"0\" max=\"23\" value=\"0\" oninput=\"updateScheduleDetailEnd()\"></div><div><label class=\"field\">Duration minutes</label><input id=\"sdDurM\" type=\"number\" min=\"0\" max=\"59\" value=\"0\" oninput=\"updateScheduleDetailEnd()\"></div></div><label class=\"field\">After the duration</label><div class=\"time-readout\" id=\"sdEndReadout\">—</div><label class=\"field\">Repeat on</label><div class=\"days\" id=\"scheduleDays\"></div><label class=\"small\" style=\"display:block;margin-top:15px\"><input id=\"sdEnabled\" type=\"checkbox\"> Enabled</label><div class=\"bar\"><button class=\"primary\" onclick=\"saveScheduleDetail()\">Save</button></div><div id=\"scheduleDetailMsg\" class=\"msg\"></div><div class=\"bar\"><button onclick=\"deleteScheduleDetail()\">Delete</button></div></div></section>\n"
"</div></aside>\n"
"<script>\n"
"const RELAY_TOTAL=7,days=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];let relayCfg=[],states=[],rooms=['Room 1','Room 2','Room 3'],relayRequests=Array(RELAY_TOTAL).fill(null),relayTimers=Array(RELAY_TOTAL).fill(null),relaySeq=Array(RELAY_TOTAL).fill(0),relayPending=Array(RELAY_TOTAL).fill(false),schedules=[],userOffline=false,controlsBuilt=false;\n"
"const $=id=>document.getElementById(id);const esc=s=>String(s==null?'':s).replace(/[&<>'\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',\"'\":'&#39;','\"':'&quot;'}[c]));\n"
"function applyTheme(t){if(t==='auto')document.documentElement.removeAttribute('data-theme');else document.documentElement.setAttribute('data-theme',t);document.querySelectorAll('#themeSeg button').forEach(b=>b.classList.toggle('active',b.dataset.th===t))}function setTheme(t){try{localStorage.setItem('theme',t)}catch(e){}applyTheme(t)}applyTheme((()=>{try{return localStorage.getItem('theme')||'auto'}catch(e){return'auto'}})());\n"
"function fitBrand(){const el=$('brandTitle');if(!el)return;const box=el.parentElement;let s=24;el.style.fontSize=s+'px';while(el.scrollWidth>box.clientWidth&&s>14){el.style.fontSize=(--s)+'px'}}\n"
"function relayCard(i,compact=false){const r=relayCfg[i];if(!r||!r.enabled)return'';const on=!!states[i],cl=compact?' compact-relay':'';return `<div class=\"relay-card${cl}\" data-i=\"${i}\" onclick=\"rowToggle(${i})\"><div class=\"relay-meta\"><div class=\"relay-name\">${esc(r.name)}</div><div class=\"relay-state\" id=\"st${i}\">${on?'ON':'OFF'}</div></div><span class=\"switch\"><input type=\"checkbox\" id=\"r${i}\" ${on?'checked':''} tabindex=\"-1\"><span class=\"slider\"></span></span></div>`}\n"
"function roomCard(key,label,relayIndices,open){const active=relayIndices.filter(i=>relayCfg[i]?.enabled).length;const summary=active+' control'+(active===1?'':'s');return `<section class=\"room-card${open?' open':''}\" id=\"${key}Card\"><button class=\"room-head\" onclick=\"toggleRoom('${key}')\"><span class=\"room-badge\">${key.replace('room','R')}</span><span class=\"room-title-wrap\"><span class=\"room-title\">${esc(label)}</span><span class=\"room-summary\">${summary}</span></span><svg class=\"room-chevron\" viewBox=\"0 0 24 24\"><path d=\"m6 9 6 6 6-6\"></path></svg></button><div class=\"room-body\"><div class=\"room-body-inner\"><div class=\"room-controls\">${relayIndices.map(i=>relayCard(i,true)).join('')||'<div class=\"empty\">No enabled relay</div>'}</div></div></div></section>`}\n"
"function render(){const main1=relayCard(0),main2=relayCard(1);$('controls').innerHTML=`<div class=\"grid-2\"><div class=\"main-relay\">${main1}</div><div class=\"main-relay\">${main2}</div></div>${roomCard('room2',rooms[1],[3],false)}<div class=\"grid-2\">${roomCard('room1',rooms[0],[2,4,5],false)}${roomCard('room3',rooms[2],[0,1,6],false)}</div>`;controlsBuilt=true}\n"
"function updateStates(){for(let i=0;i<RELAY_TOTAL;i++){if(relayPending[i])continue;const on=!!states[i],el=$('r'+i),st=$('st'+i);if(el)el.checked=on;if(st)st.textContent=on?'ON':'OFF'}}function toggleRoom(id){$(id+'Card').classList.toggle('open')}\n"
"function rowToggle(i){setRelay(i,!states[i])}\n"
"function setRelay(i,on){if(!relayCfg[i]?.enabled)return;const seq=++relaySeq[i],target=on?1:0,previous=states[i]?1:0;states[i]=target;relayPending[i]=true;updateStates();if(relayTimers[i])clearTimeout(relayTimers[i]);if(relayRequests[i]){try{relayRequests[i].abort()}catch(e){}}relayTimers[i]=setTimeout(()=>{const ctl=new AbortController();relayRequests[i]=ctl;fetch(`/api/relay?relay=${i+1}&state=${target}`,{cache:'no-store',signal:ctl.signal}).then(r=>{if(!r.ok)throw Error();if(seq===relaySeq[i])relayPending[i]=false}).catch(e=>{if(e.name==='AbortError')return;if(seq===relaySeq[i]){relayPending[i]=false;states[i]=previous;updateStates()}})},35)}\n"
"function connSvg(type){if(type==='online')return '<path d=\"M5 12a10 10 0 0 1 14 0\"></path><path d=\"M8.5 15a5 5 0 0 1 7 0\"></path><path d=\"M12 18h.01\"></path>';if(type==='trying')return '<path d=\"M4 12a11 11 0 0 1 16 0\"></path><path d=\"M7 15a7 7 0 0 1 10 0\"></path><path d=\"m12 18 .01\"></path>';return '<path d=\"M7 7 17 17\"></path><path d=\"M4 12a11 11 0 0 1 16 0\"></path><path d=\"M7 15a7 7 0 0 1 10 0\"></path>'}\n"
"function updateOnline(wifi,cloud,offline,timeSynced){userOffline=!!offline;const b=$('connToggle'),state=offline?'offline':(wifi?'online':'trying');b.className='conn-toggle '+state;b.title=offline?'Tap to go online':(wifi?'Tap to go offline':'Connecting to Wi-Fi');$('connIcon').innerHTML=connSvg(state)}\n"
"async function toggleConnectivity(){const b=$('connToggle');b.disabled=true;try{const r=await fetch('/api/connectivity',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({offline:!userOffline})});if(r.ok)await load()}catch(e){}finally{b.disabled=false}}\n"
"async function load(){try{const r=await fetch('/api/status',{cache:'no-store'});if(!r.ok)throw 0;const d=await r.json();relayCfg=d.config||relayCfg;states=d.states||states;rooms=d.roomNames||rooms;if(!controlsBuilt)render();else updateStates();const bt=d.brandName||'Smart Home';if($('brandTitle').textContent!==bt){$('brandTitle').textContent=bt;fitBrand()}document.title=bt;updateOnline(d.wifiConnected,d.cloudOnline,d.userOffline,d.timeSynced);$('diagOptionalGpio').textContent=(d.optionalGpios||[]).join(', ')||'None';updateDiagnostics(d)}catch(e){updateOnline(false,false,userOffline,false)}}\n"
"function updateDiagnostics(d){if(!$('diagPage').classList.contains('active'))return;$('diagMdns').textContent=(d.mdnsHost||'smarthome')+'.local';$('diagRssi').textContent=d.rssi==null?'Not connected':d.rssi+' dBm';$('diagHeap').textContent=d.freeHeap!=null?Math.round(d.freeHeap/1024)+' KB':'—';$('diagMinHeap').textContent=d.minFreeHeap!=null?Math.round(d.minFreeHeap/1024)+' KB':'—';if(d.uptimeSeconds!=null){const s=d.uptimeSeconds,dy=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);$('diagUptime').textContent=(dy?dy+'d ':'')+h+'h '+m+'m'}}\n"
"function openSettings(){document.body.classList.add('settings-open');$('drawerBackdrop').classList.add('open');$('settingsDrawer').classList.add('open');$('settingsDrawer').setAttribute('aria-hidden','false');showSettingsHome();load()};function closeSettings(){const d=$('settingsDrawer');d.classList.remove('open');$('drawerBackdrop').classList.remove('open');d.setAttribute('aria-hidden','true');setTimeout(showSettingsHome,300)}function showSettingsHome(){document.querySelectorAll('.subpage').forEach(p=>p.classList.remove('active'));$('settingsHome').classList.add('active')};function openSubPage(id){document.querySelectorAll('.subpage').forEach(p=>p.classList.remove('active'));$(id).classList.add('active');if(id==='relayPage')renderRelayConfig();if(id==='brandPage')loadBrand();if(id==='apPage')loadSettings();if(id==='internetPage')loadWifiStatus();if(id==='remotePage')loadCloudStatus();if(id==='mqttPage')loadMqtt();if(id==='schedulePage')loadSchedules();if(id==='diagPage')load()};function backToSettings(){showSettingsHome()}\n"
"function renderRelayConfig(){let h='';relayCfg.forEach((r,i)=>{h+=`<div class=\"relay-config-item\"><div class=\"relay-config-head\"><div><div class=\"relay-number\">Relay ${i+1}</div><div class=\"relay-gpio\">Relay GPIO ${r.gpio}</div><div class=\"relay-switch-gpio\">Physical Switch ${r.switchGpio>=0?'GPIO '+r.switchGpio:'None'}</div></div><label class=\"small-switch\"><input type=\"checkbox\" id=\"en${i}\" ${r.enabled?'checked':''}><span class=\"small-slider\"></span></label></div><label class=\"field\">Name</label><input type=\"text\" id=\"rn${i}\" maxlength=\"31\" value=\"${esc(r.name)}\"></div>`});$('relayConfigList').innerHTML=h;rooms.forEach((n,i)=>{const el=$('room'+(i+1)+'Name');if(el)el.value=n||('Room '+(i+1))})}\n"
"async function saveRelayConfig(){const body={};for(let i=0;i<RELAY_TOTAL;i++){body['r'+(i+1)+'_enabled']=$('en'+i).checked;const name=$('rn'+i).value.trim()||('Relay '+(i+1));if(name.length>31)return $('relaymsg').textContent='Relay name is too long.';body['r'+(i+1)+'_name']=name}for(let i=0;i<3;i++){const n=$('room'+(i+1)+'Name').value.trim()||('Room '+(i+1));if(n.length>24)return $('relaymsg').textContent='Room name is too long.';body['room'+(i+1)+'_name']=n}$('relaymsg').textContent='Saving…';try{const r=await fetch('/api/relays',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}),d=await r.json().catch(()=>({}));if(!r.ok)throw Error(d.error||'Save failed');relayCfg=d.config||relayCfg;rooms=d.roomNames||rooms;controlsBuilt=false;render();$('relaymsg').textContent='Saved.'}catch(e){$('relaymsg').textContent=e.message||'Save failed.'}}\n"
"function loadBrand(){fetch('/api/status',{cache:'no-store'}).then(r=>r.json()).then(d=>$('brandInput').value=d.brandName||'Smart Home').catch(()=>{})}async function saveBrand(){const name=$('brandInput').value.trim();if(!name||name.length>40)return $('brandMsg').textContent='Enter 1-40 characters.';try{const r=await fetch('/api/brand',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({brandName:name})}),d=await r.json();if(!r.ok)throw Error(d.error||'Save failed');$('brandTitle').textContent=name;fitBrand();$('brandMsg').textContent='Saved.'}catch(e){$('brandMsg').textContent=e.message||'Save failed.'}}\n"
"function loadSettings(){fetch('/api/settings',{cache:'no-store'}).then(r=>r.json()).then(d=>$('ssid').value=d.ssid||'').catch(()=>{})}async function saveSettings(){const ssid=$('ssid').value,pass=$('pass').value;if(ssid.length<1||ssid.length>32||pass.length<8||pass.length>63)return $('setmsg').textContent='Invalid SSID or password.';$('setmsg').textContent='Saving and restarting…';try{await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ssid,password:pass})})}catch(e){$('setmsg').textContent='Connection lost. The AP may be restarting.'}}\n"
"async function loadWifiStatus(){try{const d=await (await fetch('/api/internet',{cache:'no-store'})).json();$('staSsid').value=d.staSsid||'';$('staPass').value='';$('wifiStatus').textContent=!d.wifiConfigured?'Wi-Fi not configured.':d.connected?'Connected.':'Waiting for connection.'}catch(e){}}async function saveWifiSta(){const ssid=$('staSsid').value.trim(),pass=$('staPass').value,m=$('wifiMsg');if(ssid.length<1||ssid.length>32||pass.length<8||pass.length>63)return m.textContent='Invalid Wi-Fi credentials.';try{const r=await fetch('/api/wifi-sta',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ssid,password:pass})});if(!r.ok)throw Error('Save failed');m.textContent='Saved. Connecting…';setTimeout(loadWifiStatus,2500)}catch(e){m.textContent=e.message||'Save failed.'}}\n"
"async function loadCloudStatus(){try{const d=await (await fetch('/api/internet',{cache:'no-store'})).json();$('cloudUrl').value=d.cloudUrl||'';$('deviceId').value=d.deviceId||'';$('deviceToken').value='';$('cloudStatus').textContent=!d.cloudConfigured?'Remote access off.':d.connected?'Connected.':'Waiting for Wi-Fi.'}catch(e){}}async function saveCloud(){const url=$('cloudUrl').value.trim(),id=$('deviceId').value.trim(),token=$('deviceToken').value.trim(),m=$('cloudMsg');const any=url||id||token;if(any&&(!url||!id||!token||!url.startsWith('https://')))return m.textContent='Provide URL, Device ID and token, or leave all blank.';try{const r=await fetch('/api/internet',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({cloudUrl:url,deviceId:id,deviceToken:token})}),d=await r.json().catch(()=>({}));if(!r.ok)throw Error(d.error||'Save failed');m.textContent=any?'Saved.':'Disabled.';loadCloudStatus()}catch(e){m.textContent=e.message||'Save failed.'}}\n"
"async function loadMqtt(){try{const d=await (await fetch('/api/mqtt',{cache:'no-store'})).json();$('mqttHost').value=d.mqttHost||'';$('mqttPort').value=d.mqttPort||1883;$('mqttUser').value=d.mqttUser||'';$('mqttPass').value='';$('mqttEnabled').checked=!!d.mqttEnabled;$('mqttStatus').textContent=!d.mqttEnabled?'MQTT off.':d.mqttConnected?'Connected.':'Waiting for broker.'}catch(e){}}async function saveMqtt(){const host=$('mqttHost').value.trim(),port=+$('mqttPort').value,user=$('mqttUser').value.trim(),pass=$('mqttPass').value,enabled=$('mqttEnabled').checked,m=$('mqttMsg');if(enabled&&!host)return m.textContent='Enter broker host or IP.';if(port<1||port>65535)return m.textContent='Invalid port.';try{const r=await fetch('/api/mqtt',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mqttHost:host,mqttPort:port,mqttUser:user,mqttPass:pass,mqttEnabled:enabled})}),d=await r.json().catch(()=>({}));if(!r.ok)throw Error(d.error||'Save failed');m.textContent=enabled?'Saved.':'Disabled.';setTimeout(loadMqtt,1800)}catch(e){m.textContent=e.message||'Save failed.'}}\n"
"function timeFromMinutes(m){m=((m%1440)+1440)%1440;return String(Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0')}function buildTimeOptions(sel,count){let o='';for(let i=0;i<count;i++)o+=`<option value=\"${i}\" ${i===sel?'selected':''}>${String(i).padStart(2,'0')}</option>`;return o}function buildHour12Options(sel){let o='';for(let i=1;i<=12;i++)o+=`<option value=\"${i}\" ${i===sel?'selected':''}>${i}</option>`;return o}\n"
"let currentScheduleIndex=-1,scheduleIsNew=false;function renderSchedules(){$('scheduleList').innerHTML=schedules.map((s,i)=>`<div class=\"list-row${s.enabled===false||s.enabled===0?' row-disabled':''}\" onclick=\"openScheduleDetail(${i})\"><div><strong>Relay ${Number(s.relay||1)} • ${Number(s.action||0)?'ON':'OFF'}</strong><span class=\"list-row-sub\">${(Number(s.hour)||0).toString().padStart(2,'0')}:${String(Number(s.minute)||0).padStart(2,'0')}</span></div><span class=\"chevron\">›</span></div>`).join('')||'<div class=\"small\">No schedules yet.</div>'}\n"
"async function loadSchedules(){try{const r=await fetch('/api/schedules',{cache:'no-store'}),d=await r.json();if(!r.ok)throw Error(d.error||'Could not load schedules');schedules=d.schedules||[];renderSchedules();$('scheduleMsg').textContent=schedules.length+' schedule(s) stored.'}catch(e){$('scheduleMsg').textContent=e.message||'Could not load schedules.'}}function addSchedule(){if(schedules.length>=64)return $('scheduleMsg').textContent='Maximum 64 schedules reached.';schedules.push({relay:1,hour:0,minute:0,action:1,durationMinutes:0,days:127,enabled:true});openScheduleDetail(schedules.length-1,true)}\n"
"function openScheduleDetail(idx,isNew){currentScheduleIndex=idx;scheduleIsNew=!!isNew;const s=schedules[idx]||{},relay=Number(s.relay||1),h=Number(s.hour||0),mi=Number(s.minute||0),dur=Math.max(0,Math.min(1439,Number(s.durationMinutes||0))),act=Number(s.action==null?1:s.action),en=s.enabled!==false&&s.enabled!==0,bits=Number(s.days==null?127:s.days);$('sdRelay').innerHTML=Array.from({length:RELAY_TOTAL},(_,i)=>`<option value=\"${i+1}\" ${relay===i+1?'selected':''}>Relay ${i+1}</option>`).join('');$('sdAction').innerHTML=`<option value=\"1\" ${act===1?'selected':''}>Turn ON</option><option value=\"0\" ${act===0?'selected':''}>Turn OFF</option>`;$('sdHour').innerHTML=buildHour12Options(h%12===0?12:h%12);$('sdMinute').innerHTML=buildTimeOptions(mi,60);$('sdAmPm').innerHTML=`<option value=\"0\" ${h<12?'selected':''}>AM</option><option value=\"1\" ${h>=12?'selected':''}>PM</option>`;$('sdDurH').value=Math.floor(dur/60);$('sdDurM').value=dur%60;$('sdEnabled').checked=en;$('scheduleDays').innerHTML=days.map((d,i)=>`<label><input class=\"day\" type=\"checkbox\" data-day=\"${i}\" ${(bits&(1<<i))?'checked':''}>${d}</label>`).join('');$('scheduleDetailMsg').textContent='';updateScheduleDetailEnd();openSubPage('scheduleDetailPage')}\n"
"function readScheduleDetail(){const relay=+$('sdRelay').value,action=+$('sdAction').value,h12=+$('sdHour').value,ap=+$('sdAmPm').value,minute=+$('sdMinute').value;let hour=h12%12;if(ap===1)hour+=12;let daysMask=0;document.querySelectorAll('#scheduleDays input.day').forEach(x=>{if(x.checked)daysMask|=1<<Number(x.dataset.day)});const durH=Math.max(0,Math.min(23,Number($('sdDurH').value)||0)),durM=Math.max(0,Math.min(59,Number($('sdDurM').value)||0));return{relay,hour,minute,action,durationMinutes:Math.min(1439,durH*60+durM),days:daysMask,enabled:$('sdEnabled').checked}}\n"
"function updateScheduleDetailEnd(){const s=readScheduleDetail(),label=s.action===1?'Automatically turns OFF at ':'Automatically turns ON at ';$('sdEndReadout').textContent=s.durationMinutes?label+timeFromMinutes(s.hour*60+s.minute+s.durationMinutes):'No automatic revert.'}\n"
"async function saveScheduleDetail(){const s=readScheduleDetail();if(!s.days)return $('scheduleDetailMsg').textContent='Select at least one day.';schedules[currentScheduleIndex]=s;try{const r=await fetch('/api/schedules',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({schedules})}),d=await r.json().catch(()=>({}));if(!r.ok)throw Error(d.error||'Save failed');scheduleIsNew=false;$('scheduleDetailMsg').textContent='Saved.';renderSchedules();openSubPage('schedulePage')}catch(e){$('scheduleDetailMsg').textContent=e.message||'Save failed.'}}\n"
"async function deleteScheduleDetail(){schedules.splice(currentScheduleIndex,1);try{await fetch('/api/schedules',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({schedules})})}catch(e){}renderSchedules();openSubPage('schedulePage')}\n"
"function backFromScheduleDetail(){if(scheduleIsNew)schedules.splice(currentScheduleIndex,1);scheduleIsNew=false;renderSchedules();openSubPage('schedulePage')}\n"
"fitBrand();load();setInterval(load,1500);window.addEventListener('resize',fitBrand);\n"
"</script>\n"
"</body></html>\n"
;

static bool valid_ssid(const char *s)
{
    size_t n = strnlen(s, MAX_AP_SSID_LEN + 1);
    return n >= 1 && n <= MAX_AP_SSID_LEN;
}

static bool valid_password(const char *s)
{
    size_t n = strnlen(s, MAX_AP_PASS_LEN + 1);
    return n >= 8 && n <= MAX_AP_PASS_LEN;
}

static bool valid_relay_name(const char *s)
{
    size_t n = strnlen(s, MAX_RELAY_NAME_LEN + 1);
    if (n < 1 || n > MAX_RELAY_NAME_LEN) return false;

    
    for (size_t i = 0; i < n; ++i) {
        unsigned char c = (unsigned char)s[i];
        if (c < 0x20 || c == 0x7F || c == '"' || c == '\\') return false;
    }
    return true;
}

static bool valid_brand_name(const char *s)
{
    size_t n = strnlen(s, MAX_BRAND_LEN + 1);
    if (n < 1 || n > MAX_BRAND_LEN) return false;
    for (size_t i = 0; i < n; ++i) {
        unsigned char c = (unsigned char)s[i];
        if (c < 0x20 || c == 0x7F || c == '"' || c == '\\') return false;
    }
    return true;
}

static esp_err_t save_brand_name(const char *name)
{
    if (!valid_brand_name(name)) return ESP_ERR_INVALID_ARG;
    xSemaphoreTake(storage_mutex, portMAX_DELAY);
    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_str(h, NVS_KEY_BRAND_NAME, name);
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    xSemaphoreGive(storage_mutex);
    if (err == ESP_OK) strlcpy(brand_name, name, sizeof(brand_name));
    return err;
}

static void load_defaults(void)
{
    strlcpy(brand_name, "Smart Home", sizeof(brand_name));
    strlcpy(ap_ssid, DEFAULT_AP_SSID, sizeof(ap_ssid));
    strlcpy(ap_password, DEFAULT_AP_PASSWORD, sizeof(ap_password));

    for (int i = 0; i < RELAY_COUNT; ++i) {
        relay_state[i] = 0;
        relay_enabled[i] = true;
        snprintf(relay_name[i], sizeof(relay_name[i]), "Relay %d", i + 1);
    }
    strlcpy(room_name[0], "Room 1", sizeof(room_name[0]));
    strlcpy(room_name[1], "Room 2", sizeof(room_name[1]));
    strlcpy(room_name[2], "Room 3", sizeof(room_name[2]));
}

static void load_nvs(void)
{
    load_defaults();

    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READONLY, &h);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "No existing config; using defaults");
        return;
    }

    uint8_t states[RELAY_COUNT] = {0};
    size_t sz = sizeof(states);
    if (nvs_get_blob(h, NVS_KEY_RELAY_STATES, states, &sz) == ESP_OK && sz == sizeof(states)) {
        for (int i = 0; i < RELAY_COUNT; ++i) relay_state[i] = states[i] ? 1 : 0;
    }

    uint8_t enabled[RELAY_COUNT] = {1, 1, 1, 1, 1, 1, 1};
    sz = sizeof(enabled);
    if (nvs_get_blob(h, NVS_KEY_RELAY_ENABLED, enabled, &sz) == ESP_OK && sz == sizeof(enabled)) {
        for (int i = 0; i < RELAY_COUNT; ++i) {
            relay_enabled[i] = enabled[i] != 0;
        }
    }

    size_t names_sz = sizeof(relay_name);
    if (nvs_get_blob(h, NVS_KEY_RELAY_NAMES, relay_name, &names_sz) == ESP_OK &&
        names_sz == sizeof(relay_name)) {
        for (int i = 0; i < RELAY_COUNT; ++i) {
            relay_name[i][MAX_RELAY_NAME_LEN] = '\0';
            if (!valid_relay_name(relay_name[i])) {
                snprintf(relay_name[i], sizeof(relay_name[i]), "Relay %d", i + 1);
            }
        }
    }

    char tmp_ssid[MAX_AP_SSID_LEN + 1] = {0};
    sz = sizeof(tmp_ssid);
    if (nvs_get_str(h, NVS_KEY_AP_SSID, tmp_ssid, &sz) == ESP_OK && valid_ssid(tmp_ssid)) {
        strlcpy(ap_ssid, tmp_ssid, sizeof(ap_ssid));
    }

    char tmp_pass[MAX_AP_PASS_LEN + 1] = {0};
    sz = sizeof(tmp_pass);
    if (nvs_get_str(h, NVS_KEY_AP_PASS, tmp_pass, &sz) == ESP_OK && valid_password(tmp_pass)) {
        strlcpy(ap_password, tmp_pass, sizeof(ap_password));
    }

    char tmp_brand[MAX_BRAND_LEN + 1] = {0};
    sz = sizeof(tmp_brand);
    if (nvs_get_str(h, NVS_KEY_BRAND_NAME, tmp_brand, &sz) == ESP_OK &&
        strlen(tmp_brand) >= 1 && strlen(tmp_brand) <= MAX_BRAND_LEN) {
        strlcpy(brand_name, tmp_brand, sizeof(brand_name));
    }

    size_t rooms_sz = sizeof(room_name);
    if (nvs_get_blob(h, NVS_KEY_ROOM_NAMES, room_name, &rooms_sz) == ESP_OK && rooms_sz == sizeof(room_name)) {
        for (int i = 0; i < 3; ++i) {
            room_name[i][MAX_ROOM_NAME_LEN] = '\0';
            if (strlen(room_name[i]) < 1 || strlen(room_name[i]) > MAX_ROOM_NAME_LEN)
                snprintf(room_name[i], sizeof(room_name[i]), "Room %d", i + 1);
        }
    }

    char tmp_sta_ssid[MAX_AP_SSID_LEN + 1] = {0};
    sz = sizeof(tmp_sta_ssid);
    if (nvs_get_str(h, NVS_KEY_STA_SSID, tmp_sta_ssid, &sz) == ESP_OK && valid_ssid(tmp_sta_ssid)) strlcpy(sta_ssid, tmp_sta_ssid, sizeof(sta_ssid));
    char tmp_sta_pass[MAX_AP_PASS_LEN + 1] = {0};
    sz = sizeof(tmp_sta_pass);
    if (nvs_get_str(h, NVS_KEY_STA_PASS, tmp_sta_pass, &sz) == ESP_OK && valid_password(tmp_sta_pass)) strlcpy(sta_password, tmp_sta_pass, sizeof(sta_password));
    sz = sizeof(cloud_url);
    if (nvs_get_str(h, NVS_KEY_CLOUD_URL, cloud_url, &sz) != ESP_OK) cloud_url[0] = '\0';
    sz = sizeof(device_id);
    if (nvs_get_str(h, NVS_KEY_DEVICE_ID, device_id, &sz) != ESP_OK) device_id[0] = '\0';
    sz = sizeof(device_token);
    if (nvs_get_str(h, NVS_KEY_DEVICE_TOKEN, device_token, &sz) != ESP_OK) device_token[0] = '\0';

    sz = sizeof(mqtt_host);
    if (nvs_get_str(h, NVS_KEY_MQTT_HOST, mqtt_host, &sz) != ESP_OK) mqtt_host[0] = '\0';
    sz = sizeof(mqtt_user);
    if (nvs_get_str(h, NVS_KEY_MQTT_USER, mqtt_user, &sz) != ESP_OK) mqtt_user[0] = '\0';
    sz = sizeof(mqtt_pass);
    if (nvs_get_str(h, NVS_KEY_MQTT_PASS, mqtt_pass, &sz) != ESP_OK) mqtt_pass[0] = '\0';
    uint16_t tmp_port = 0;
    if (nvs_get_u16(h, NVS_KEY_MQTT_PORT, &tmp_port) == ESP_OK && tmp_port > 0) mqtt_port = tmp_port;
    uint8_t tmp_en = 0;
    if (nvs_get_u8(h, NVS_KEY_MQTT_ENABLED, &tmp_en) == ESP_OK) mqtt_enabled = tmp_en != 0;

    nvs_close(h);
    ESP_LOGI(TAG, "Internet config: STA=%s cloud=%s device=%s", sta_ssid[0] ? "configured" : "not configured", cloud_url[0] ? cloud_url : "none", device_id[0] ? device_id : "none");
    ESP_LOGI(TAG, "Restored relay states: %d %d %d %d %d %d %d",
             relay_state[0], relay_state[1], relay_state[2], relay_state[3], relay_state[4], relay_state[5], relay_state[6]);
    ESP_LOGI(TAG, "Relay enabled: %d %d %d %d %d %d %d",
             relay_enabled[0], relay_enabled[1], relay_enabled[2], relay_enabled[3], relay_enabled[4], relay_enabled[5], relay_enabled[6]);
}

static esp_err_t save_relay_states(void)
{
    uint8_t states[RELAY_COUNT];
    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    for (int i = 0; i < RELAY_COUNT; ++i) states[i] = relay_state[i] ? 1 : 0;
    xSemaphoreGive(relay_mutex);

    xSemaphoreTake(storage_mutex, portMAX_DELAY);
    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_blob(h, NVS_KEY_RELAY_STATES, states, sizeof(states));
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    xSemaphoreGive(storage_mutex);

    if (err != ESP_OK) ESP_LOGE(TAG, "Relay NVS save failed: %s", esp_err_to_name(err));
    return err;
}

static void relay_save_task(void *arg)
{
    (void)arg;
    while (1) {
        
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        vTaskDelay(pdMS_TO_TICKS(250));
        while (ulTaskNotifyTake(pdTRUE, 0) > 0) {}
        for (int attempt = 0; attempt < 3; ++attempt) {
            if (save_relay_states() == ESP_OK) break;
            vTaskDelay(pdMS_TO_TICKS(200));
        }
    }
}

static esp_err_t save_relay_config(void)
{
    uint8_t enabled[RELAY_COUNT];
    uint8_t states[RELAY_COUNT];
    char names[RELAY_COUNT][MAX_RELAY_NAME_LEN + 1];
    char rooms[3][MAX_ROOM_NAME_LEN + 1];

    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    for (int i = 0; i < RELAY_COUNT; ++i) {
        enabled[i] = relay_enabled[i] ? 1 : 0;
        states[i] = relay_state[i] ? 1 : 0;
    }
    memcpy(names, relay_name, sizeof(names));
    memcpy(rooms, room_name, sizeof(rooms));
    xSemaphoreGive(relay_mutex);

    xSemaphoreTake(storage_mutex, portMAX_DELAY);
    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_blob(h, NVS_KEY_RELAY_ENABLED, enabled, sizeof(enabled));
        if (err == ESP_OK) err = nvs_set_blob(h, NVS_KEY_RELAY_NAMES, names, sizeof(names));
        if (err == ESP_OK) err = nvs_set_blob(h, NVS_KEY_ROOM_NAMES, rooms, sizeof(rooms));
        if (err == ESP_OK) err = nvs_set_blob(h, NVS_KEY_RELAY_STATES, states, sizeof(states));
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    xSemaphoreGive(storage_mutex);

    if (err != ESP_OK) ESP_LOGE(TAG, "Relay config NVS save failed: %s", esp_err_to_name(err));
    return err;
}

static esp_err_t save_ap_settings(const char *ssid, const char *password)
{
    xSemaphoreTake(storage_mutex, portMAX_DELAY);
    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_str(h, NVS_KEY_AP_SSID, ssid);
        if (err == ESP_OK) err = nvs_set_str(h, NVS_KEY_AP_PASS, password);
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    xSemaphoreGive(storage_mutex);

    if (err == ESP_OK) {
        strlcpy(ap_ssid, ssid, sizeof(ap_ssid));
        strlcpy(ap_password, password, sizeof(ap_password));
    }
    return err;
}


static esp_err_t save_wifi_sta_settings(const char *ssid, const char *pass)
{
    xSemaphoreTake(storage_mutex, portMAX_DELAY);
    nvs_handle_t h; esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_str(h, NVS_KEY_STA_SSID, ssid);
        if (err == ESP_OK) err = nvs_set_str(h, NVS_KEY_STA_PASS, pass);
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    xSemaphoreGive(storage_mutex);
    if (err == ESP_OK) {
        strlcpy(sta_ssid, ssid, sizeof(sta_ssid));
        strlcpy(sta_password, pass, sizeof(sta_password));
    }
    return err;
}

static esp_err_t save_cloud_settings(const char *url, const char *id, const char *token)
{
    xSemaphoreTake(storage_mutex, portMAX_DELAY);
    nvs_handle_t h; esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_str(h, NVS_KEY_CLOUD_URL, url);
        if (err == ESP_OK) err = nvs_set_str(h, NVS_KEY_DEVICE_ID, id);
        if (err == ESP_OK) err = nvs_set_str(h, NVS_KEY_DEVICE_TOKEN, token);
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    xSemaphoreGive(storage_mutex);
    if (err == ESP_OK) {
        strlcpy(cloud_url, url, sizeof(cloud_url));
        strlcpy(device_id, id, sizeof(device_id));
        strlcpy(device_token, token, sizeof(device_token));
    }
    return err;
}

static esp_err_t save_mqtt_settings(const char *host, uint16_t port, const char *user, const char *pass, bool enabled)
{
    xSemaphoreTake(storage_mutex, portMAX_DELAY);
    nvs_handle_t h; esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_str(h, NVS_KEY_MQTT_HOST, host);
        if (err == ESP_OK) err = nvs_set_u16(h, NVS_KEY_MQTT_PORT, port);
        if (err == ESP_OK) err = nvs_set_str(h, NVS_KEY_MQTT_USER, user);
        if (err == ESP_OK) err = nvs_set_str(h, NVS_KEY_MQTT_PASS, pass);
        if (err == ESP_OK) err = nvs_set_u8(h, NVS_KEY_MQTT_ENABLED, enabled ? 1 : 0);
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    xSemaphoreGive(storage_mutex);
    if (err == ESP_OK) {
        strlcpy(mqtt_host, host, sizeof(mqtt_host));
        mqtt_port = port;
        strlcpy(mqtt_user, user, sizeof(mqtt_user));
        strlcpy(mqtt_pass, pass, sizeof(mqtt_pass));
        mqtt_enabled = enabled;
    }
    return err;
}

static gpio_num_t relay_gpio(int index);
static void mqtt_publish_relay_state(int idx);
static void mqtt_start(void);
static void mqtt_stop(void);
static void mqtt_publish_ha_discovery(void);
static void mqtt_publish_all_states(void);
static int relay_output_level(int logical_state);

static void apply_remote_relay_state(int index, int state)
{
    if (index < 0 || index >= RELAY_COUNT) return;
    bool changed = false;
    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    if (relay_enabled[index] && relay_state[index] != (state ? 1 : 0)) {
        relay_state[index] = state ? 1 : 0;
        gpio_set_level(relay_gpio(index), relay_output_level(relay_state[index]));
        changed = true;
    }
    xSemaphoreGive(relay_mutex);
    if (changed) { xTaskNotifyGive(relay_save_task_handle); mqtt_publish_relay_state(index); }
}

static void get_relay_snapshot(int *states, bool *enabled)
{
    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    memcpy(states, relay_state, sizeof(int) * RELAY_COUNT);
    memcpy(enabled, relay_enabled, sizeof(bool) * RELAY_COUNT);
    xSemaphoreGive(relay_mutex);
}

static void cloud_command_cb(int relay, int state, void *ctx)
{
    apply_remote_relay_state(relay, state);
}

static void cloud_snapshot_cb(int *states, bool *enabled, void *ctx)
{
    get_relay_snapshot(states, enabled);
}

static int relay_output_level(int logical_state)
{
    return logical_state ? RELAY_ACTIVE_LEVEL : !RELAY_ACTIVE_LEVEL;
}

static gpio_num_t relay_gpio(int index)
{
    static const gpio_num_t pins[RELAY_COUNT] = {
        RELAY1_GPIO, RELAY2_GPIO, RELAY3_GPIO, RELAY4_GPIO, RELAY5_GPIO, RELAY6_GPIO, RELAY7_GPIO
    };
    return pins[index];
}

static void apply_all_relays(void)
{
    int s[RELAY_COUNT];
    bool enabled[RELAY_COUNT];

    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    memcpy(s, relay_state, sizeof(s));
    memcpy(enabled, relay_enabled, sizeof(enabled));
    xSemaphoreGive(relay_mutex);

    for (int i = 0; i < RELAY_COUNT; ++i) {
        gpio_set_level(relay_gpio(i),
                       (enabled[i] && s[i]) ? RELAY_ACTIVE_LEVEL : !RELAY_ACTIVE_LEVEL);
    }
}

static void init_relays(void)
{
    uint64_t mask = 0;
    for (int i = 0; i < RELAY_COUNT; ++i) mask |= (1ULL << relay_gpio(i));

    gpio_config_t io = {
        .pin_bit_mask = mask,
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    ESP_ERROR_CHECK(gpio_config(&io));

    
    for (int i = 0; i < RELAY_COUNT; ++i) {
        gpio_set_level(relay_gpio(i), relay_output_level(0));
    }
}

static gpio_num_t switch_gpio(int index)
{
    static const gpio_num_t pins[SWITCH_COUNT] = {
        SWITCH1_GPIO, SWITCH2_GPIO, SWITCH3_GPIO, SWITCH4_GPIO, SWITCH5_GPIO
    };
    if (index < 0 || index >= SWITCH_COUNT) return (gpio_num_t)-1;
    return pins[index];
}

static bool read_switch_state(int index)
{
    gpio_num_t pin = switch_gpio(index);
    return pin != (gpio_num_t)-1 && gpio_get_level(pin) == SWITCH_ACTIVE_LEVEL;
}

static void init_switches(void)
{
    uint64_t mask = 0;
    for (int i = 0; i < SWITCH_COUNT; ++i) mask |= (1ULL << switch_gpio(i));

    gpio_config_t io = {
        .pin_bit_mask = mask,
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    ESP_ERROR_CHECK(gpio_config(&io));
}

static void apply_switch_command(int index, bool on)
{
    bool changed = false;

    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    if (relay_enabled[index] && relay_state[index] != (int)on) {
        relay_state[index] = on ? 1 : 0;
        gpio_set_level(relay_gpio(index), relay_output_level(on ? 1 : 0));
        changed = true;
    }
    xSemaphoreGive(relay_mutex);
    if (changed) schedule_note_manual_change(index);

    if (changed) {
        
        
        xTaskNotifyGive(relay_save_task_handle);
        mqtt_publish_relay_state(index);
    }
}

static void physical_switch_task(void *arg)
{
    int last_raw[SWITCH_COUNT];
    int stable[SWITCH_COUNT];
    uint8_t samples[SWITCH_COUNT] = {0};

    esp_task_wdt_add(NULL);

    for (int i = 0; i < SWITCH_COUNT; ++i) {
        last_raw[i] = gpio_get_level(switch_gpio(i));
        
        stable[i] = last_raw[i];
        samples[i] = SWITCH_DEBOUNCE_SAMPLES;
    }

    while (1) {
        for (int i = 0; i < SWITCH_COUNT; ++i) {
            int raw = gpio_get_level(switch_gpio(i));

            if (raw == last_raw[i]) {
                if (samples[i] < SWITCH_DEBOUNCE_SAMPLES) samples[i]++;
            } else {
                last_raw[i] = raw;
                samples[i] = 0;
            }

            if (samples[i] >= SWITCH_DEBOUNCE_SAMPLES && stable[i] != raw) {
                stable[i] = raw;
                apply_switch_command(i, raw == SWITCH_ACTIVE_LEVEL);
                ESP_LOGI(TAG, "Physical switch %d -> %s", i + 1,
                         (raw == SWITCH_ACTIVE_LEVEL) ? "ON" : "OFF");
            }
        }

        esp_task_wdt_reset();
        vTaskDelay(pdMS_TO_TICKS(SWITCH_POLL_MS));
    }
}

static void sta_reconnect_task(void *arg)
{
    (void)arg;
    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        if (!sta_ssid[0] || user_offline_mode || sta_connected) continue;

        uint8_t retry = sta_retry_count;
        uint32_t delay_s = 1;
        if (retry >= 2) delay_s = 2;
        if (retry >= 3) delay_s = 4;
        if (retry >= 4) delay_s = 8;
        if (retry >= 5) delay_s = 16;
        if (retry >= 6) delay_s = 30;
        if (delay_s > 30) delay_s = 30;

        vTaskDelay(pdMS_TO_TICKS(delay_s * 1000));
        if (!sta_ssid[0] || user_offline_mode || sta_connected) continue;
        esp_err_t err = esp_wifi_connect();
        if (err != ESP_OK) ESP_LOGW(TAG, "STA reconnect request failed (retry %u): %s", (unsigned)sta_retry_count, esp_err_to_name(err));
    }
}

static void wifi_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg;
    (void)data;
    if (base != WIFI_EVENT) return;

    if (id == WIFI_EVENT_AP_START) {
        ESP_LOGI(TAG, "AP started");
    } else if (id == WIFI_EVENT_AP_STACONNECTED) {
        ESP_LOGI(TAG, "Local client connected");
    } else if (id == WIFI_EVENT_AP_STADISCONNECTED) {
        ESP_LOGI(TAG, "Local client disconnected");
    } else if (id == WIFI_EVENT_STA_START) {
        sta_retry_count = 0;
        if (sta_reconnect_task_handle) xTaskNotifyGive(sta_reconnect_task_handle);
    } else if (id == WIFI_EVENT_STA_DISCONNECTED) {
        sta_connected = false;
        sta_ip[0] = '\0';
        if (!sta_ssid[0] || user_offline_mode) return;
        if (sta_retry_count < 10) sta_retry_count++;
        ESP_LOGW(TAG, "STA disconnected; scheduling retry %u", (unsigned)sta_retry_count);
        if (sta_reconnect_task_handle) xTaskNotifyGive(sta_reconnect_task_handle);
    }
}

static bool g_sntp_started = false;

static void ip_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg;
    if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        sta_retry_count = 0;
        sta_connected = true;
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)data;
        snprintf(sta_ip, sizeof(sta_ip), IPSTR, IP2STR(&event->ip_info.ip));
        ESP_LOGI(TAG, "STA connected; internet features enabled; local IP: %s", sta_ip);
        if (!g_sntp_started) {
            g_sntp_started = true;
            esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
            esp_sntp_setservername(0, "pool.ntp.org");
            esp_sntp_init();
        }
        if (mqtt_enabled && mqtt_host[0] && !mqtt_client) mqtt_start();
    }
}

static void wifi_init_ap_sta(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_t *ap_netif = esp_netif_create_default_wifi_ap();
    esp_netif_t *sta_netif = esp_netif_create_default_wifi_sta();
    if (!ap_netif || !sta_netif) ESP_ERROR_CHECK(ESP_FAIL);

    esp_netif_ip_info_t ip_info;
    ESP_ERROR_CHECK(esp_netif_get_ip_info(ap_netif, &ip_info));
    ESP_ERROR_CHECK(esp_netif_str_to_ip4(AP_IP_ADDR, &ip_info.ip));
    ESP_ERROR_CHECK(esp_netif_str_to_ip4(AP_GW_ADDR, &ip_info.gw));
    ESP_ERROR_CHECK(esp_netif_str_to_ip4(AP_NETMASK, &ip_info.netmask));
    ESP_ERROR_CHECK(esp_netif_dhcps_stop(ap_netif));
    ESP_ERROR_CHECK(esp_netif_set_ip_info(ap_netif, &ip_info));
    ESP_ERROR_CHECK(esp_netif_dhcps_start(ap_netif));

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    esp_event_handler_instance_t w_any, ip_any;
    ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, &w_any));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &ip_event_handler, NULL, &ip_any));

    wifi_config_t ap = {0};
    strlcpy((char *)ap.ap.ssid, ap_ssid, sizeof(ap.ap.ssid));
    strlcpy((char *)ap.ap.password, ap_password, sizeof(ap.ap.password));
    ap.ap.ssid_len = strlen(ap_ssid); ap.ap.channel = DEFAULT_AP_CHANNEL;
    ap.ap.max_connection = AP_MAX_CONNECTIONS; ap.ap.authmode = WIFI_AUTH_WPA2_PSK;
    ap.ap.pmf_cfg.required = false; ap.ap.pmf_cfg.capable = true;

    wifi_config_t sta = {0};
    if (sta_ssid[0]) {
        strlcpy((char *)sta.sta.ssid, sta_ssid, sizeof(sta.sta.ssid));
        strlcpy((char *)sta.sta.password, sta_password, sizeof(sta.sta.password));
        
        sta.sta.scan_method = WIFI_ALL_CHANNEL_SCAN;
        sta.sta.sort_method = WIFI_CONNECT_AP_BY_SIGNAL;
        sta.sta.threshold.authmode = WIFI_AUTH_OPEN;
        sta.sta.pmf_cfg.capable = true;
        sta.sta.pmf_cfg.required = false;
        sta.sta.failure_retry_cnt = 7;
    }
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_APSTA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap));
    if (sta_ssid[0]) ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta));
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    if (xTaskCreate(sta_reconnect_task, "sta_reconnect", 3072, NULL, 3, &sta_reconnect_task_handle) != pdPASS) {
        ESP_LOGE(TAG, "STA reconnect task creation failed");
    }
    ESP_ERROR_CHECK(esp_wifi_start());
    esp_wifi_set_max_tx_power(60);
    ESP_LOGI(TAG, "AP SSID: %s", ap_ssid);
    ESP_LOGI(TAG, "AP IP: %s", AP_IP_ADDR);
}

static int find_question_end(const uint8_t *buf, int len)
{
    if (len < 17) return -1;
    int p = 12;
    int jumps = 0;
    while (p < len && jumps++ < 64) {
        uint8_t l = buf[p++];
        if (l == 0) {
            if (p + 4 > len) return -1;
            return p + 4;
        }
        if ((l & 0xC0) != 0 || l > 63 || p + l > len) return -1;
        p += l;
    }
    return -1;
}

static int build_dns_answer(uint8_t *out, int out_cap, const uint8_t *query, int qlen)
{
    int qend = find_question_end(query, qlen);
    if (qend < 0 || qend + 16 > out_cap || qend > qlen) return -1;

    memcpy(out, query, qend);
    out[2] = 0x81; out[3] = 0x80;
    out[4] = 0x00; out[5] = 0x01;
    out[6] = 0x00; out[7] = 0x01;
    out[8] = out[9] = out[10] = out[11] = 0;

    int p = qend;
    out[p++] = 0xC0; out[p++] = 0x0C;
    out[p++] = 0x00; out[p++] = 0x01;
    out[p++] = 0x00; out[p++] = 0x01;
    out[p++] = 0x00; out[p++] = 0x00; out[p++] = 0x00; out[p++] = 0x3C;
    out[p++] = 0x00; out[p++] = 0x04;
    out[p++] = 192; out[p++] = 168; out[p++] = 4; out[p++] = 1;
    return p;
}

static void dns_task(void *arg)
{
    uint8_t rx[DNS_RX_SIZE];
    uint8_t tx[DNS_RX_SIZE + 32];

    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock < 0) {
        ESP_LOGE(TAG, "DNS socket failed");
        vTaskDelete(NULL);
        return;
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(DNS_PORT);
    addr.sin_addr.s_addr = inet_addr(AP_IP_ADDR);

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        ESP_LOGE(TAG, "DNS bind failed: errno=%d", errno);
        close(sock);
        vTaskDelete(NULL);
        return;
    }

    struct timeval tv = {.tv_sec = 1, .tv_usec = 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    ESP_LOGI(TAG, "Local DNS started on UDP/53");
    esp_task_wdt_add(NULL);

    while (1) {
        struct sockaddr_in from;
        socklen_t from_len = sizeof(from);
        int n = recvfrom(sock, rx, sizeof(rx), 0, (struct sockaddr *)&from, &from_len);
        if (n <= 0) {
            esp_task_wdt_reset();
            continue;
        }

        int out_len = build_dns_answer(tx, sizeof(tx), rx, n);
        if (out_len > 0) {
            sendto(sock, tx, out_len, 0, (struct sockaddr *)&from, from_len);
        }
        esp_task_wdt_reset();
    }
}

static esp_err_t send_json(httpd_req_t *req, const char *json, const char *status)
{
    httpd_resp_set_status(req, status);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, json, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t send_cjson(httpd_req_t *req, cJSON *root, const char *status)
{
    if (!root) return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error");
    char *text = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!text) return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error");
    esp_err_t err = send_json(req, text, status);
    free(text);
    return err;
}

static esp_err_t redirect_to_root(httpd_req_t *req)
{
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", "/");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, NULL, 0);
}

static esp_err_t root_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, HTML_PAGE, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t status_handler(httpd_req_t *req)
{
    int s[RELAY_COUNT]; bool enabled[RELAY_COUNT];
    char names[RELAY_COUNT][MAX_RELAY_NAME_LEN + 1];
    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    memcpy(s, relay_state, sizeof(s)); memcpy(enabled, relay_enabled, sizeof(enabled)); memcpy(names, relay_name, sizeof(names));
    xSemaphoreGive(relay_mutex);

    cJSON *root = cJSON_CreateObject(), *states = cJSON_CreateArray(), *config = cJSON_CreateArray();
    if (!root || !states || !config) { if(root)cJSON_Delete(root); if(states)cJSON_Delete(states); if(config)cJSON_Delete(config); return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error"); }
    for (int i=0;i<RELAY_COUNT;i++) {
        cJSON_AddItemToArray(states,cJSON_CreateNumber(s[i]));
        cJSON *o=cJSON_CreateObject();
        if(!o){cJSON_Delete(root);cJSON_Delete(states);cJSON_Delete(config);return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error");}
        cJSON_AddBoolToObject(o,"enabled",enabled[i]); cJSON_AddStringToObject(o,"name",names[i]);
        cJSON_AddNumberToObject(o,"gpio",relay_gpio(i)); cJSON_AddNumberToObject(o,"switchGpio",switch_gpio(i)); cJSON_AddItemToArray(config,o);
    }
    cJSON *rooms = cJSON_CreateArray();
    cJSON *optional = cJSON_CreateArray();
    if (!rooms || !optional) { if (rooms) cJSON_Delete(rooms); if (optional) cJSON_Delete(optional); cJSON_Delete(root); cJSON_Delete(states); cJSON_Delete(config); return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error"); }
    for (int i = 0; i < 3; ++i) cJSON_AddItemToArray(rooms, cJSON_CreateString(room_name[i]));
    const int used_relays[] = {RELAY1_GPIO, RELAY2_GPIO, RELAY3_GPIO, RELAY4_GPIO, RELAY5_GPIO, RELAY6_GPIO, RELAY7_GPIO, SWITCH1_GPIO, SWITCH2_GPIO, SWITCH3_GPIO, SWITCH4_GPIO, SWITCH5_GPIO};
    const size_t used_count = sizeof(used_relays) / sizeof(used_relays[0]);
    static const int optional_candidates[] = {0, 1, 2, 3, 4, 5, 12, 13, 14, 15};
    for (size_t i = 0; i < sizeof(optional_candidates) / sizeof(optional_candidates[0]); ++i) {
        int pin = optional_candidates[i];
        bool used = false;
        for (size_t j = 0; j < used_count; ++j) if (used_relays[j] == pin) { used = true; break; }
        if (!used) cJSON_AddItemToArray(optional, cJSON_CreateNumber(pin));
    }
    cJSON_AddItemToObject(root,"states",states); cJSON_AddItemToObject(root,"config",config); cJSON_AddItemToObject(root,"roomNames",rooms); cJSON_AddItemToObject(root,"optionalGpios",optional);
    cJSON_AddStringToObject(root,"brandName",brand_name); cJSON_AddBoolToObject(root,"wifiConnected",sta_connected);
    cJSON_AddBoolToObject(root,"cloudOnline",cloud_client_is_online()); cJSON_AddBoolToObject(root,"userOffline",user_offline_mode);
    cJSON_AddBoolToObject(root,"timeSynced",g_time_synced); cJSON_AddStringToObject(root,"staIp",sta_ip);
    cJSON_AddNumberToObject(root,"freeHeap",(double)esp_get_free_heap_size());
    cJSON_AddNumberToObject(root,"minFreeHeap",(double)esp_get_minimum_free_heap_size());
    cJSON_AddNumberToObject(root,"uptimeSeconds",(double)(esp_timer_get_time()/1000000LL));
    int rssi = 0;
    if (sta_connected && esp_wifi_sta_get_rssi(&rssi) == ESP_OK) cJSON_AddNumberToObject(root,"rssi",rssi);
    else cJSON_AddNullToObject(root,"rssi");
    cJSON_AddStringToObject(root,"mdnsHost",mdns_host);
    return send_cjson(req,root,"200 OK");
}

static esp_err_t relay_handler(httpd_req_t *req)
{

    char query[128];
    char value[20];

    if (httpd_req_get_url_query_str(req, query, sizeof(query)) != ESP_OK)
        return send_json(req, "{\"error\":\"missing query\"}", "400 Bad Request");

    if (httpd_query_key_value(query, "relay", value, sizeof(value)) != ESP_OK)
        return send_json(req, "{\"error\":\"relay\"}", "400 Bad Request");

    char *end = NULL;
    long relay = strtol(value, &end, 10);
    if (*value == '\0' || *end != '\0' || relay < 1 || relay > RELAY_COUNT)
        return send_json(req, "{\"error\":\"relay\"}", "400 Bad Request");

    if (httpd_query_key_value(query, "state", value, sizeof(value)) != ESP_OK)
        return send_json(req, "{\"error\":\"state\"}", "400 Bad Request");

    end = NULL;
    long state = strtol(value, &end, 10);
    if (*value == '\0' || *end != '\0' || (state != 0 && state != 1))
        return send_json(req, "{\"error\":\"state\"}", "400 Bad Request");

    int idx = (int)relay - 1;

    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    if (!relay_enabled[idx]) {
        xSemaphoreGive(relay_mutex);
        return send_json(req, "{\"error\":\"relay disabled\"}", "409 Conflict");
    }

    bool changed = relay_state[idx] != (int)state;
    if (changed) {
        relay_state[idx] = (int)state;
        gpio_set_level(relay_gpio(idx), relay_output_level((int)state));
    }
    xSemaphoreGive(relay_mutex);
    if (changed) {
        schedule_note_manual_change(idx);
        xTaskNotifyGive(relay_save_task_handle);
        mqtt_publish_relay_state(idx);
    }
    return send_json(req, "{\"ok\":true}", "200 OK");
}

static bool json_extract_string(const char *body, const char *key, char *out, size_t out_sz);

static esp_err_t connectivity_post_handler(httpd_req_t *req)
{
    if (req->content_len <= 0 || req->content_len > 256)
        return send_json(req, "{\"error\":\"invalid body\"}", "400 Bad Request");

    char body[257];
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) return ESP_FAIL;
        received += (size_t)n;
    }
    body[received] = '\0';

    cJSON *root = cJSON_Parse(body);
    if (!root) return send_json(req, "{\"error\":\"invalid JSON\"}", "400 Bad Request");
    cJSON *v = cJSON_GetObjectItem(root, "offline");
    bool offline = cJSON_IsBool(v) ? cJSON_IsTrue(v) : false;
    cJSON_Delete(root);

    user_offline_mode = offline;
    cloud_client_set_offline(offline);

    if (offline) {
        if (sta_ssid[0]) esp_wifi_disconnect();
    } else {
        if (sta_ssid[0] && sta_reconnect_task_handle) xTaskNotifyGive(sta_reconnect_task_handle);
    }

    return send_json(req, offline ? "{\"ok\":true,\"userOffline\":true}" : "{\"ok\":true,\"userOffline\":false}", "200 OK");
}

static esp_err_t internet_get_handler(httpd_req_t *req)
{
    cJSON *root=cJSON_CreateObject(); if(!root)return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error");
    cJSON_AddStringToObject(root,"staSsid",sta_ssid); cJSON_AddStringToObject(root,"cloudUrl",cloud_url);
    cJSON_AddStringToObject(root,"deviceId",device_id); cJSON_AddStringToObject(root,"staIp",sta_ip);
    cJSON_AddBoolToObject(root,"wifiConfigured",sta_ssid[0]!='\0');
    cJSON_AddBoolToObject(root,"cloudConfigured",cloud_url[0]!='\0'&&device_id[0]!='\0'&&device_token[0]!='\0');
    cJSON_AddBoolToObject(root,"connected",sta_connected); return send_cjson(req,root,"200 OK");
}

static esp_err_t wifi_sta_post_handler(httpd_req_t *req)
{

    if (req->content_len <= 0 || req->content_len > 512)
        return send_json(req, "{\"error\":\"invalid body\"}", "400 Bad Request");

    char body[513];
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) return ESP_FAIL;
        received += (size_t)n;
    }
    body[received] = '\0';

    char ssid[MAX_AP_SSID_LEN + 1] = {0};
    char pass[MAX_AP_PASS_LEN + 1] = {0};

    if (!json_extract_string(body, "ssid", ssid, sizeof(ssid)) ||
        !json_extract_string(body, "password", pass, sizeof(pass)) ||
        !valid_ssid(ssid) || !valid_password(pass)) {
        return send_json(req, "{\"error\":\"valid Wi-Fi SSID/password are required\"}",
                         "400 Bad Request");
    }

    if (save_wifi_sta_settings(ssid, pass) != ESP_OK)
        return send_json(req, "{\"error\":\"save failed\"}", "500 Internal Server Error");

    wifi_config_t sta_cfg = {0};
    strlcpy((char *)sta_cfg.sta.ssid, ssid, sizeof(sta_cfg.sta.ssid));
    strlcpy((char *)sta_cfg.sta.password, pass, sizeof(sta_cfg.sta.password));
    sta_cfg.sta.scan_method = WIFI_ALL_CHANNEL_SCAN;

    esp_wifi_disconnect();
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta_cfg));
    sta_retry_count = 0;
    if (!user_offline_mode && sta_reconnect_task_handle) xTaskNotifyGive(sta_reconnect_task_handle);

    return send_json(req, "{\"ok\":true,\"restarting\":false}", "200 OK");
}

static esp_err_t internet_post_handler(httpd_req_t *req)
{

    if (req->content_len <= 0 || req->content_len > 1024)
        return send_json(req, "{\"error\":\"invalid body\"}", "400 Bad Request");

    char body[1025];
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) return ESP_FAIL;
        received += (size_t)n;
    }
    body[received] = '\0';

    char url[MAX_CLOUD_URL_LEN + 1] = {0};
    char id[MAX_DEVICE_ID_LEN + 1] = {0};
    char token[MAX_DEVICE_TOKEN_LEN + 1] = {0};

    bool have_url = json_extract_string(body, "cloudUrl", url, sizeof(url)) && url[0];
    bool have_id = json_extract_string(body, "deviceId", id, sizeof(id)) && id[0];
    bool have_token = json_extract_string(body, "deviceToken", token, sizeof(token)) && token[0];

    if (!have_url && !have_id && !have_token) {
        url[0] = 0; id[0] = 0; token[0] = 0;
    } else if (!have_url || !have_id || !have_token ||
        strncmp(url, "https://", 8) != 0 ||
        strlen(id) < 3 || strlen(id) > MAX_DEVICE_ID_LEN ||
        strlen(token) < 16) {
        return send_json(req,
                         "{\"error\":\"provide Cloud URL, Device ID and Device Token together; HTTPS is required\"}",
                         "400 Bad Request");
    }

    if (save_cloud_settings(url, id, token) != ESP_OK)
        return send_json(req, "{\"error\":\"save failed\"}", "500 Internal Server Error");

    cloud_client_set_credentials(url, id, token);

    return send_json(req, "{\"ok\":true,\"restarting\":false}", "200 OK");
}

static esp_err_t mqtt_get_handler(httpd_req_t *req)
{
    cJSON *root=cJSON_CreateObject(); if(!root)return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error");
    cJSON_AddStringToObject(root,"mqttHost",mqtt_host);
    cJSON_AddNumberToObject(root,"mqttPort",mqtt_port);
    cJSON_AddStringToObject(root,"mqttUser",mqtt_user);
    cJSON_AddBoolToObject(root,"mqttEnabled",mqtt_enabled);
    cJSON_AddBoolToObject(root,"mqttConnected",mqtt_connected);
    cJSON_AddStringToObject(root,"mqttBaseTopic",mqtt_base_topic);
    return send_cjson(req,root,"200 OK");
}

static esp_err_t mqtt_post_handler(httpd_req_t *req)
{
    if (req->content_len <= 0 || req->content_len > 512)
        return send_json(req, "{\"error\":\"invalid body\"}", "400 Bad Request");

    char body[513];
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) return ESP_FAIL;
        received += (size_t)n;
    }
    body[received] = '\0';

    char host[MAX_MQTT_HOST_LEN + 1] = {0};
    char user[MAX_MQTT_USER_LEN + 1] = {0};
    char pass[MAX_MQTT_PASS_LEN + 1] = {0};
    long port = 1883;
    bool enabled = false;

    cJSON *j = cJSON_Parse(body);
    if (!j) return send_json(req, "{\"error\":\"invalid JSON\"}", "400 Bad Request");
    cJSON *h = cJSON_GetObjectItem(j, "mqttHost");
    cJSON *p = cJSON_GetObjectItem(j, "mqttPort");
    cJSON *u = cJSON_GetObjectItem(j, "mqttUser");
    cJSON *pw = cJSON_GetObjectItem(j, "mqttPass");
    cJSON *en = cJSON_GetObjectItem(j, "mqttEnabled");
    if (cJSON_IsString(h)) strlcpy(host, h->valuestring, sizeof(host));
    if (cJSON_IsNumber(p)) port = (long)p->valuedouble;
    if (cJSON_IsString(u)) strlcpy(user, u->valuestring, sizeof(user));
    if (cJSON_IsString(pw)) strlcpy(pass, pw->valuestring, sizeof(pass));
    else strlcpy(pass, mqtt_pass, sizeof(pass));
    if (cJSON_IsBool(en)) enabled = cJSON_IsTrue(en);
    cJSON_Delete(j);

    if (port < 1 || port > 65535)
        return send_json(req, "{\"error\":\"invalid port\"}", "400 Bad Request");
    if (enabled && !host[0])
        return send_json(req, "{\"error\":\"broker host is required to enable MQTT\"}", "400 Bad Request");

    if (save_mqtt_settings(host, (uint16_t)port, user, pass, enabled) != ESP_OK)
        return send_json(req, "{\"error\":\"save failed\"}", "500 Internal Server Error");

    if (enabled && host[0]) mqtt_start(); else mqtt_stop();

    return send_json(req, "{\"ok\":true}", "200 OK");
}

static esp_err_t settings_get_handler(httpd_req_t *req)
{
    cJSON *root=cJSON_CreateObject(); if(!root)return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error");
    cJSON_AddStringToObject(root,"ssid",ap_ssid); cJSON_AddStringToObject(root,"brandName",brand_name); return send_cjson(req,root,"200 OK");
}

static bool json_extract_string(const char *body, const char *key, char *out, size_t out_sz)
{
    if (!body || !key || !out || out_sz == 0) return false;
    cJSON *root = cJSON_Parse(body);
    if (!root) return false;
    cJSON *v = cJSON_GetObjectItemCaseSensitive(root, key);
    bool ok = cJSON_IsString(v) && v->valuestring &&
              strnlen(v->valuestring, out_sz) < out_sz;
    if (ok) strlcpy(out, v->valuestring, out_sz);
    cJSON_Delete(root);
    return ok;
}

static bool json_extract_bool(const char *body, const char *key, bool *out)
{
    if (!body || !key || !out) return false;
    cJSON *root = cJSON_Parse(body);
    if (!root) return false;
    cJSON *v = cJSON_GetObjectItemCaseSensitive(root, key);
    bool ok = cJSON_IsBool(v);
    if (ok) *out = cJSON_IsTrue(v);
    cJSON_Delete(root);
    return ok;
}

static esp_err_t settings_post_handler(httpd_req_t *req)
{
    if (req->content_len <= 0 || req->content_len > 1024)
        return send_json(req, "{\"error\":\"invalid body\"}", "400 Bad Request");

    char body[1025];
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) return ESP_FAIL;
        received += (size_t)n;
    }
    body[received] = '\0';

    char ssid[MAX_AP_SSID_LEN + 1];
    char pass[MAX_AP_PASS_LEN + 1];
    if (!json_extract_string(body, "ssid", ssid, sizeof(ssid)) ||
        !json_extract_string(body, "password", pass, sizeof(pass)) ||
        !valid_ssid(ssid) || !valid_password(pass)) {
        return send_json(req, "{\"error\":\"invalid SSID/password\"}", "400 Bad Request");
    }

    if (save_ap_settings(ssid, pass) != ESP_OK)
        return send_json(req, "{\"error\":\"save failed\"}", "500 Internal Server Error");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    httpd_resp_sendstr(req, "{\"ok\":true,\"restarting\":true}");

    vTaskDelay(pdMS_TO_TICKS(700));
    esp_restart();
    return ESP_OK;
}

static void mdns_apply_hostname(void);

static esp_err_t brand_post_handler(httpd_req_t *req)
{
    if (req->content_len <= 0 || req->content_len > 512)
        return send_json(req, "{\"error\":\"invalid body\"}", "400 Bad Request");

    char body[513], name[MAX_BRAND_LEN + 1];
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) return ESP_FAIL;
        received += (size_t)n;
    }
    body[received] = '\0';
    if (!json_extract_string(body, "brandName", name, sizeof(name)) || !valid_brand_name(name))
        return send_json(req, "{\"error\":\"invalid brand name\"}", "400 Bad Request");
    if (save_brand_name(name) != ESP_OK)
        return send_json(req, "{\"error\":\"save failed\"}", "500 Internal Server Error");
    mdns_apply_hostname();
    cJSON *root = cJSON_CreateObject();
    if (!root) return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error");
    cJSON_AddBoolToObject(root, "ok", true);
    cJSON_AddStringToObject(root, "brandName", brand_name);
    return send_cjson(req, root, "200 OK");
}

static bool schedule_time_is_active(const cloud_schedule_t *s, const struct tm *tmv, int now_minute)
{
    if (!s || !s->enabled || s->duration_minutes <= 0) return false;
    int start = s->hour * 60 + s->minute;
    int elapsed;
    int day = tmv->tm_wday;
    if (now_minute >= start) {
        if (!(s->days & (1 << day))) return false;
        elapsed = now_minute - start;
    } else {
        day = (day + 6) % 7;
        if (!(s->days & (1 << day))) return false;
        elapsed = now_minute + 1440 - start;
    }
    return elapsed >= 0 && elapsed < s->duration_minutes;
}

static void schedule_set_relay(int index, int state)
{
    if (index < 0 || index >= RELAY_COUNT) return;
    bool changed = false;
    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    if (relay_enabled[index] && relay_state[index] != state) {
        relay_state[index] = state ? 1 : 0;
        gpio_set_level(relay_gpio(index), relay_output_level(relay_state[index]));
        changed = true;
    }
    xSemaphoreGive(relay_mutex);
    if (changed) { xTaskNotifyGive(relay_save_task_handle); mqtt_publish_relay_state(index); }
}

static void schedule_task(void *arg)
{
    (void)arg;
    cloud_schedule_t local[CLOUD_SCHEDULE_MAX];
    esp_task_wdt_add(NULL);
    for (;;) {
        time_t now = time(NULL);
        struct tm tmv;
        g_time_synced = (now >= 1700000000);
        if (!g_time_synced) {
            static uint32_t warn_count = 0;
            if ((warn_count++ % 30) == 0) {
                ESP_LOGW(TAG, "Clock not yet synced via NTP; schedules are paused until time sync completes");
            }
        }
        if (g_time_synced && localtime_r(&now, &tmv) != NULL) {
            size_t n = cloud_client_get_schedules(local, CLOUD_SCHEDULE_MAX);
            int now_minute = tmv.tm_hour * 60 + tmv.tm_min;
            bool explicit_event[RELAY_COUNT] = {false,false,false,false,false};
            int explicit_state[RELAY_COUNT] = {0,0,0,0,0};
            bool active[RELAY_COUNT] = {false,false,false,false,false};
            int active_state[RELAY_COUNT] = {0,0,0,0,0};

            for (size_t i = 0; i < n; ++i) {
                cloud_schedule_t *s = &local[i];
                if (!s->enabled || s->relay < 1 || s->relay > RELAY_COUNT) continue;
                int r = s->relay - 1;
                if (schedule_time_is_active(s, &tmv, now_minute)) {
                    active[r] = true;
                    active_state[r] = s->action ? 1 : 0;
                }
                if ((s->days & (1 << tmv.tm_wday)) &&
                    s->hour == tmv.tm_hour && s->minute == tmv.tm_min) {
                    explicit_event[r] = true;
                    explicit_state[r] = s->action ? 1 : 0;
                }
            }

            for (int r = 0; r < RELAY_COUNT; ++r) {
                if (explicit_event[r]) {
                    schedule_set_relay(r, explicit_state[r]);
                    schedule_was_active[r] = active[r];
                    schedule_revert_state[r] = active[r] ? (1 - active_state[r]) : explicit_state[r];
                    schedule_override[r] = false;
                } else if (active[r]) {
                    schedule_revert_state[r] = 1 - active_state[r];
                    if (!schedule_override[r]) schedule_set_relay(r, active_state[r]);
                    schedule_was_active[r] = true;
                } else if (schedule_was_active[r]) {
                    if (!schedule_override[r]) schedule_set_relay(r, schedule_revert_state[r]);
                    schedule_was_active[r] = false;
                    schedule_override[r] = false;
                }
            }
        }
        esp_task_wdt_reset();
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

static esp_err_t schedules_handler(httpd_req_t *req)
{

    if (req->method == HTTP_GET) {
        cloud_schedule_t items[CLOUD_SCHEDULE_MAX];
        size_t n = cloud_client_get_schedules(items, CLOUD_SCHEDULE_MAX);
        cJSON *root = cJSON_CreateObject();
        cJSON *arr = cJSON_CreateArray();
        if (!root || !arr) {
            if (root) cJSON_Delete(root);
            if (arr) cJSON_Delete(arr);
            return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error");
        }
        for (size_t i = 0; i < n; ++i) {
            cJSON *o = cJSON_CreateObject();
            if (!o) { cJSON_Delete(root); return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error"); }
            cJSON_AddNumberToObject(o, "id", (double)i);
            cJSON_AddBoolToObject(o, "enabled", items[i].enabled);
            cJSON_AddNumberToObject(o, "relay", items[i].relay);
            cJSON_AddNumberToObject(o, "hour", items[i].hour);
            cJSON_AddNumberToObject(o, "minute", items[i].minute);
            cJSON_AddNumberToObject(o, "action", items[i].action);
            cJSON_AddNumberToObject(o, "days", items[i].days);
            cJSON_AddNumberToObject(o, "durationMinutes", items[i].duration_minutes);
            cJSON_AddItemToArray(arr, o);
        }
        cJSON_AddItemToObject(root, "schedules", arr);
        char *out = cJSON_PrintUnformatted(root);
        cJSON_Delete(root);
        if (!out) return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error");
        esp_err_t err = send_json(req, out, "200 OK");
        free(out);
        return err;
    }

    if (req->method != HTTP_POST || req->content_len <= 0 || req->content_len > 20000)
        return send_json(req, "{\"error\":\"invalid request\"}", "400 Bad Request");

    char *body = calloc(1, req->content_len + 1);
    if (!body) return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error");
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) { free(body); return ESP_FAIL; }
        received += (size_t)n;
    }
    body[received] = '\0';

    cJSON *root = cJSON_Parse(body);
    free(body);
    if (!root) return send_json(req, "{\"error\":\"invalid JSON\"}", "400 Bad Request");

    cJSON *arr = cJSON_GetObjectItem(root, "schedules");
    int arr_count = cJSON_IsArray(arr) ? cJSON_GetArraySize(arr) : -1;
    if (arr_count < 0 || arr_count > CLOUD_SCHEDULE_MAX) {
        cJSON_Delete(root);
        return send_json(req, "{\"error\":\"maximum 64 schedules\"}", "400 Bad Request");
    }

    cloud_schedule_t *items = NULL;
    if (arr_count > 0) {
        items = calloc((size_t)arr_count, sizeof(cloud_schedule_t));
        if (!items) {
            cJSON_Delete(root);
            return send_json(req, "{\"error\":\"out of memory\"}", "500 Internal Server Error");
        }
    }
    bool valid = true;
    for (int i = 0; i < arr_count; ++i) {
        cJSON *o = cJSON_GetArrayItem(arr, i);
        cJSON *v;
        if (!cJSON_IsObject(o)) { valid = false; break; }
        items[i].id = i;
        items[i].enabled = (v=cJSON_GetObjectItem(o,"enabled")) ? cJSON_IsTrue(v) : false;
        items[i].relay = (v=cJSON_GetObjectItem(o,"relay")) ? v->valueint : 0;
        items[i].hour = (v=cJSON_GetObjectItem(o,"hour")) ? v->valueint : -1;
        items[i].minute = (v=cJSON_GetObjectItem(o,"minute")) ? v->valueint : -1;
        items[i].action = (v=cJSON_GetObjectItem(o,"action")) ? v->valueint : -1;
        items[i].days = (v=cJSON_GetObjectItem(o,"days")) ? v->valueint : 0;
        items[i].duration_minutes = (v=cJSON_GetObjectItem(o,"durationMinutes")) ? v->valueint : 0;
        if (items[i].relay < 1 || items[i].relay > RELAY_COUNT ||
            items[i].hour > 23 ||
            items[i].minute > 59 ||
            (items[i].action != 0 && items[i].action != 1) ||
            items[i].days < 1 || items[i].days > 127 ||
            items[i].duration_minutes > 1439) {
            valid = false;
            break;
        }
    }
    cJSON_Delete(root);

    if (!valid) {
        free(items);
        return send_json(req, "{\"error\":\"invalid schedule entry\"}", "400 Bad Request");
    }

    bool saved = cloud_client_replace_schedules(items, (size_t)arr_count);
    free(items);
    if (!saved)
        return send_json(req, "{\"error\":\"could not save schedules\"}", "500 Internal Server Error");

    char out[96];
    snprintf(out, sizeof(out), "{\"ok\":true,\"count\":%d}", arr_count);
    return send_json(req, out, "200 OK");
}

static esp_err_t relay_config_post_handler(httpd_req_t *req)
{
    if (req->content_len <= 0 || req->content_len > 2048)
        return send_json(req, "{\"error\":\"invalid body\"}", "400 Bad Request");

    char body[2049];
    size_t received = 0;
    while (received < (size_t)req->content_len) {
        int n = httpd_req_recv(req, body + received, req->content_len - received);
        if (n <= 0) return ESP_FAIL;
        received += (size_t)n;
    }
    body[received] = '\0';

    bool new_enabled[RELAY_COUNT];
    char new_names[RELAY_COUNT][MAX_RELAY_NAME_LEN + 1];

    for (int i = 0; i < RELAY_COUNT; ++i) {
        char key[16];

        snprintf(key, sizeof(key), "r%d_enabled", i + 1);
        if (!json_extract_bool(body, key, &new_enabled[i])) {
            return send_json(req, "{\"error\":\"invalid relay enable state\"}", "400 Bad Request");
        }

        snprintf(key, sizeof(key), "r%d_name", i + 1);
        if (!json_extract_string(body, key, new_names[i], sizeof(new_names[i])) ||
            !valid_relay_name(new_names[i])) {
            return send_json(req, "{\"error\":\"invalid relay name\"}", "400 Bad Request");
        }
    }

    char new_rooms[3][MAX_ROOM_NAME_LEN + 1];
    for (int i = 0; i < 3; ++i) {
        char rkey[20];
        snprintf(rkey, sizeof(rkey), "room%d_name", i + 1);
        if (!json_extract_string(body, rkey, new_rooms[i], sizeof(new_rooms[i])) ||
            strlen(new_rooms[i]) < 1 || strlen(new_rooms[i]) > MAX_ROOM_NAME_LEN) {
            return send_json(req, "{\"error\":\"invalid room name\"}", "400 Bad Request");
        }
    }

    bool old_enabled[RELAY_COUNT];
    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    memcpy(old_enabled, relay_enabled, sizeof(old_enabled));
    for (int i = 0; i < RELAY_COUNT; ++i) {
        relay_enabled[i] = new_enabled[i];
        strlcpy(relay_name[i], new_names[i], sizeof(relay_name[i]));

        if (!relay_enabled[i]) {
            relay_state[i] = 0;
            gpio_set_level(relay_gpio(i), relay_output_level(0));
        } else if (!old_enabled[i] && i < SWITCH_COUNT) {
            bool on = read_switch_state(i);
            relay_state[i] = on ? 1 : 0;
            gpio_set_level(relay_gpio(i), relay_output_level(on ? 1 : 0));
        }
    }
    for (int i = 0; i < 3; ++i) strlcpy(room_name[i], new_rooms[i], sizeof(room_name[i]));
    xSemaphoreGive(relay_mutex);

    esp_err_t err = save_relay_config();
    if (err != ESP_OK)
        return send_json(req, "{\"error\":\"configuration save failed\"}", "500 Internal Server Error");

    mqtt_publish_ha_discovery();
    mqtt_publish_all_states();

    
    cJSON *root = cJSON_CreateObject();
    cJSON *config = cJSON_CreateArray();
    if (!root || !config) { if(root)cJSON_Delete(root); if(config)cJSON_Delete(config); return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error"); }
    for (int i=0;i<RELAY_COUNT;i++) {
        cJSON *o=cJSON_CreateObject();
        if(!o){cJSON_Delete(root);cJSON_Delete(config);return send_json(req,"{\"error\":\"out of memory\"}","500 Internal Server Error");}
        cJSON_AddBoolToObject(o,"enabled",relay_enabled[i]); cJSON_AddStringToObject(o,"name",relay_name[i]);
        cJSON_AddNumberToObject(o,"gpio",relay_gpio(i)); cJSON_AddNumberToObject(o,"switchGpio",switch_gpio(i)); cJSON_AddItemToArray(config,o);
    }
    cJSON_AddItemToObject(root,"config",config);
    cJSON *rooms = cJSON_CreateArray();
    if (rooms) for (int i = 0; i < 3; ++i) cJSON_AddItemToArray(rooms, cJSON_CreateString(room_name[i]));
    if (rooms) cJSON_AddItemToObject(root,"roomNames",rooms);
    return send_cjson(req,root,"200 OK");
}

static esp_err_t captive_handler(httpd_req_t *req)
{
    return redirect_to_root(req);
}

static void start_http_server(void)
{
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.server_port = 80;
    config.ctrl_port = 32768;
    config.max_uri_handlers = 24;
    config.stack_size = 6144;
    config.recv_wait_timeout = 10;
    config.send_wait_timeout = 10;
    config.lru_purge_enable = true;

    if (httpd_start(&http_server, &config) != ESP_OK) {
        ESP_LOGE(TAG, "HTTP server start failed");
        return;
    }

    httpd_uri_t root = {.uri="/", .method=HTTP_GET, .handler=root_handler};
    httpd_uri_t status = {.uri="/api/status", .method=HTTP_GET, .handler=status_handler};
    httpd_uri_t relay = {.uri="/api/relay", .method=HTTP_GET, .handler=relay_handler};
    httpd_uri_t internet_get = {.uri="/api/internet", .method=HTTP_GET, .handler=internet_get_handler};
    httpd_uri_t internet_post = {.uri="/api/internet", .method=HTTP_POST, .handler=internet_post_handler};
    httpd_uri_t mqtt_get = {.uri="/api/mqtt", .method=HTTP_GET, .handler=mqtt_get_handler};
    httpd_uri_t mqtt_post = {.uri="/api/mqtt", .method=HTTP_POST, .handler=mqtt_post_handler};
    httpd_uri_t wifi_sta_post = {.uri="/api/wifi-sta", .method=HTTP_POST, .handler=wifi_sta_post_handler};
    httpd_uri_t settings_get = {.uri="/api/settings", .method=HTTP_GET, .handler=settings_get_handler};
    httpd_uri_t settings_post = {.uri="/api/settings", .method=HTTP_POST, .handler=settings_post_handler};
    httpd_uri_t brand_post = {.uri="/api/brand", .method=HTTP_POST, .handler=brand_post_handler};
    httpd_uri_t schedules_get = {.uri="/api/schedules", .method=HTTP_GET, .handler=schedules_handler};
    httpd_uri_t schedules_post = {.uri="/api/schedules", .method=HTTP_POST, .handler=schedules_handler};
    httpd_uri_t relay_config_post = {.uri="/api/relays", .method=HTTP_POST, .handler=relay_config_post_handler};
    httpd_uri_t connectivity_post = {.uri="/api/connectivity", .method=HTTP_POST, .handler=connectivity_post_handler};

    httpd_uri_t c1 = {.uri="/generate_204", .method=HTTP_GET, .handler=captive_handler};
    httpd_uri_t c2 = {.uri="/hotspot-detect.html", .method=HTTP_GET, .handler=captive_handler};
    httpd_uri_t c3 = {.uri="/connecttest.txt", .method=HTTP_GET, .handler=captive_handler};
    httpd_uri_t c4 = {.uri="/ncsi.txt", .method=HTTP_GET, .handler=captive_handler};
    httpd_uri_t c5 = {.uri="/connectivitycheck.gstatic.com/generate_204", .method=HTTP_GET, .handler=captive_handler};
    httpd_uri_t c6 = {.uri="/success.txt", .method=HTTP_GET, .handler=captive_handler};

    httpd_register_uri_handler(http_server, &root);
    httpd_register_uri_handler(http_server, &status);
    httpd_register_uri_handler(http_server, &relay);
    httpd_register_uri_handler(http_server, &internet_get);
    httpd_register_uri_handler(http_server, &internet_post);
    httpd_register_uri_handler(http_server, &mqtt_get);
    httpd_register_uri_handler(http_server, &mqtt_post);
    httpd_register_uri_handler(http_server, &wifi_sta_post);
    httpd_register_uri_handler(http_server, &settings_get);
    httpd_register_uri_handler(http_server, &settings_post);
    httpd_register_uri_handler(http_server, &brand_post);
    httpd_register_uri_handler(http_server, &schedules_get);
    httpd_register_uri_handler(http_server, &schedules_post);
    httpd_register_uri_handler(http_server, &relay_config_post);
    httpd_register_uri_handler(http_server, &connectivity_post);
    httpd_register_uri_handler(http_server, &c1);
    httpd_register_uri_handler(http_server, &c2);
    httpd_register_uri_handler(http_server, &c3);
    httpd_register_uri_handler(http_server, &c4);
    httpd_register_uri_handler(http_server, &c5);
    httpd_register_uri_handler(http_server, &c6);

    ESP_LOGI(TAG, "HTTP server ready");
}

static void mdns_hostname_from_brand(char *out, size_t out_sz)
{
    if (out_sz < 2) return;
    size_t j = 0;
    bool last_dash = false;
    for (size_t i = 0; brand_name[i] && j < out_sz - 1; ++i) {
        unsigned char c = (unsigned char)brand_name[i];
        char lc = 0;
        if (c >= 'A' && c <= 'Z') lc = (char)(c - 'A' + 'a');
        else if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) lc = (char)c;
        if (lc) { out[j++] = lc; last_dash = false; }
        else if (!last_dash && j > 0) { out[j++] = '-'; last_dash = true; }
    }
    while (j > 0 && out[j-1] == '-') j--;
    out[j] = '\0';
    if (j == 0) strlcpy(out, "smarthome", out_sz);
}

static bool g_mdns_started = false;

static void mdns_apply_hostname(void)
{
    mdns_hostname_from_brand(mdns_host, sizeof(mdns_host));
    if (!g_mdns_started) {
        if (mdns_init() != ESP_OK) { ESP_LOGW(TAG, "mDNS init failed"); return; }
        g_mdns_started = true;
        mdns_service_add(NULL, "_http", "_tcp", 80, NULL, 0);
    }
    mdns_hostname_set(mdns_host);
    mdns_instance_name_set(brand_name);
    ESP_LOGI(TAG, "mDNS hostname: %s.local", mdns_host);
}

static void mqtt_build_base_topic(void)
{
    uint8_t mac[6] = {0};
    esp_wifi_get_mac(WIFI_IF_STA, mac);
    char slug[MAX_BRAND_LEN + 8];
    mdns_hostname_from_brand(slug, sizeof(slug));
    snprintf(mqtt_base_topic, sizeof(mqtt_base_topic), "smarthome/%s-%02x%02x%02x", slug, mac[3], mac[4], mac[5]);
}

static void mqtt_publish_ha_discovery(void)
{
    if (!mqtt_client || !mqtt_connected) return;
    for (int i = 0; i < RELAY_COUNT; ++i) {
        if (!relay_enabled[i]) continue;
        char uniq[128], cmd_t[128], state_t[128], avail_t[128];
        snprintf(uniq, sizeof(uniq), "%s_relay%d", mqtt_base_topic, i + 1);
        for (char *p = uniq; *p; ++p) if (*p == '/') *p = '_';
        snprintf(cmd_t, sizeof(cmd_t), "%s/relay/%d/set", mqtt_base_topic, i + 1);
        snprintf(state_t, sizeof(state_t), "%s/relay/%d/state", mqtt_base_topic, i + 1);
        snprintf(avail_t, sizeof(avail_t), "%s/status", mqtt_base_topic);

        cJSON *root = cJSON_CreateObject();
        if (!root) continue;
        char name[MAX_RELAY_NAME_LEN + 1];
        xSemaphoreTake(relay_mutex, portMAX_DELAY);
        strlcpy(name, relay_name[i], sizeof(name));
        xSemaphoreGive(relay_mutex);
        cJSON_AddStringToObject(root, "name", name);
        cJSON_AddStringToObject(root, "unique_id", uniq);
        cJSON_AddStringToObject(root, "command_topic", cmd_t);
        cJSON_AddStringToObject(root, "state_topic", state_t);
        cJSON_AddStringToObject(root, "availability_topic", avail_t);
        cJSON_AddStringToObject(root, "payload_on", "ON");
        cJSON_AddStringToObject(root, "payload_off", "OFF");
        cJSON_AddStringToObject(root, "payload_available", "online");
        cJSON_AddStringToObject(root, "payload_not_available", "offline");
        cJSON *dev = cJSON_AddObjectToObject(root, "device");
        if (dev) {
            cJSON *ids = cJSON_AddArrayToObject(dev, "identifiers");
            if (ids) cJSON_AddItemToArray(ids, cJSON_CreateString(mqtt_base_topic));
            cJSON_AddStringToObject(dev, "name", brand_name);
            cJSON_AddStringToObject(dev, "manufacturer", "ESP32 Smart Home");
        }
        char *payload = cJSON_PrintUnformatted(root);
        cJSON_Delete(root);
        if (!payload) continue;
        char cfg_topic[192];
        snprintf(cfg_topic, sizeof(cfg_topic), "homeassistant/switch/%s/config", uniq);
        esp_mqtt_client_publish(mqtt_client, cfg_topic, payload, 0, 1, true);
        free(payload);
    }
}

static void mqtt_publish_relay_state(int idx)
{
    if (!mqtt_client || !mqtt_connected || idx < 0 || idx >= RELAY_COUNT) return;
    char topic[128];
    snprintf(topic, sizeof(topic), "%s/relay/%d/state", mqtt_base_topic, idx + 1);
    int state;
    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    state = relay_state[idx];
    xSemaphoreGive(relay_mutex);
    esp_mqtt_client_publish(mqtt_client, topic, state ? "ON" : "OFF", 0, 1, true);
}

static void mqtt_publish_all_states(void)
{
    for (int i = 0; i < RELAY_COUNT; ++i) if (relay_enabled[i]) mqtt_publish_relay_state(i);
}

static void mqtt_set_relay_from_topic(const char *topic, const char *data, int data_len)
{
    size_t base_len = strlen(mqtt_base_topic);
    if (strncmp(topic, mqtt_base_topic, base_len) != 0) return;
    const char *rest = topic + base_len;
    int idx = -1;
    if (sscanf(rest, "/relay/%d/set", &idx) != 1) return;
    idx -= 1;
    if (idx < 0 || idx >= RELAY_COUNT) return;
    bool on = (data_len == 2 && strncasecmp(data, "ON", 2) == 0) ||
              (data_len == 1 && data[0] == '1');
    bool off = (data_len == 3 && strncasecmp(data, "OFF", 3) == 0) ||
               (data_len == 1 && data[0] == '0');
    if (!on && !off) return;

    xSemaphoreTake(relay_mutex, portMAX_DELAY);
    bool changed = relay_enabled[idx] && relay_state[idx] != (on ? 1 : 0);
    if (changed) {
        relay_state[idx] = on ? 1 : 0;
        gpio_set_level(relay_gpio(idx), relay_output_level(relay_state[idx]));
    }
    xSemaphoreGive(relay_mutex);
    if (changed) {
        schedule_note_manual_change(idx);
        xTaskNotifyGive(relay_save_task_handle);
        mqtt_publish_relay_state(idx);
    }
}

static void mqtt_event_handler(void *arg, esp_event_base_t base, int32_t event_id, void *event_data)
{
    (void)arg; (void)base;
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;
    switch (event_id) {
    case MQTT_EVENT_CONNECTED: {
        mqtt_connected = true;
        char avail_t[128], sub_t[128];
        snprintf(avail_t, sizeof(avail_t), "%s/status", mqtt_base_topic);
        esp_mqtt_client_publish(mqtt_client, avail_t, "online", 0, 1, true);
        snprintf(sub_t, sizeof(sub_t), "%s/relay/+/set", mqtt_base_topic);
        esp_mqtt_client_subscribe(mqtt_client, sub_t, 1);
        mqtt_publish_ha_discovery();
        mqtt_publish_all_states();
        ESP_LOGI(TAG, "MQTT connected, base topic: %s", mqtt_base_topic);
        break;
    }
    case MQTT_EVENT_DISCONNECTED:
        mqtt_connected = false;
        break;
    case MQTT_EVENT_DATA: {
        char topic[96] = {0};
        int tlen = event->topic_len < (int)sizeof(topic) - 1 ? event->topic_len : (int)sizeof(topic) - 1;
        memcpy(topic, event->topic, tlen);
        topic[tlen] = '\0';
        mqtt_set_relay_from_topic(topic, event->data, event->data_len);
        break;
    }
    default:
        break;
    }
}

static void mqtt_stop(void)
{
    if (mqtt_client) {
        esp_mqtt_client_stop(mqtt_client);
        esp_mqtt_client_destroy(mqtt_client);
        mqtt_client = NULL;
    }
    mqtt_connected = false;
}

static void mqtt_start(void)
{
    if (!mqtt_enabled || !mqtt_host[0]) { mqtt_stop(); return; }
    mqtt_stop();
    mqtt_build_base_topic();

    char uri[MAX_MQTT_HOST_LEN + 16];
    snprintf(uri, sizeof(uri), "mqtt://%s:%u", mqtt_host, (unsigned)mqtt_port);

    char avail_t[128];
    snprintf(avail_t, sizeof(avail_t), "%s/status", mqtt_base_topic);

    esp_mqtt_client_config_t cfg = {0};
    cfg.broker.address.uri = uri;
    cfg.credentials.username = mqtt_user[0] ? mqtt_user : NULL;
    cfg.credentials.authentication.password = mqtt_pass[0] ? mqtt_pass : NULL;
    cfg.session.last_will.topic = avail_t;
    cfg.session.last_will.msg = "offline";
    cfg.session.last_will.qos = 1;
    cfg.session.last_will.retain = true;
    cfg.network.reconnect_timeout_ms = 8000;

    mqtt_client = esp_mqtt_client_init(&cfg);
    if (!mqtt_client) { ESP_LOGE(TAG, "MQTT client init failed"); return; }
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
    ESP_LOGI(TAG, "MQTT starting: %s", uri);
}

void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    relay_mutex = xSemaphoreCreateMutex();
    storage_mutex = xSemaphoreCreateMutex();
    if (!relay_mutex || !storage_mutex) {
        ESP_LOGE(TAG, "Mutex allocation failed");
        abort();
    }

    load_nvs();
    init_relays();
    init_switches();
    apply_all_relays();

    
    esp_task_wdt_config_t wdt_config = {
        .timeout_ms = WATCHDOG_TIMEOUT_MS,
        .idle_core_mask = (1U << portNUM_PROCESSORS) - 1U,
        .trigger_panic = true
    };
    ret = esp_task_wdt_init(&wdt_config);
    if (ret != ESP_OK && ret != ESP_ERR_INVALID_STATE) {
        ESP_ERROR_CHECK(ret);
    }

    BaseType_t save_ok = xTaskCreate(relay_save_task, "relay_save", 3072, NULL, 2, &relay_save_task_handle);
    if (save_ok != pdPASS) {
        ESP_LOGE(TAG, "Relay save task creation failed");
        abort();
    }

    BaseType_t switch_ok = xTaskCreate(physical_switch_task, "physical_switches", 3072, NULL, 4, &switch_task_handle);
    if (switch_ok != pdPASS) {
        ESP_LOGE(TAG, "Physical switch task creation failed");
    }

    wifi_init_ap_sta();
    mdns_apply_hostname();

    BaseType_t ok = xTaskCreate(dns_task, "local_dns", DNS_STACK_SIZE, NULL, 3, &dns_task_handle);
    if (ok != pdPASS) {
        ESP_LOGE(TAG, "DNS task creation failed");
    }

    start_http_server();

    cloud_client_config_t ccfg = {0};
    strlcpy(ccfg.base_url, cloud_url, sizeof(ccfg.base_url));
    strlcpy(ccfg.device_id, device_id, sizeof(ccfg.device_id));
    strlcpy(ccfg.device_token, device_token, sizeof(ccfg.device_token));
    ccfg.command_cb = cloud_command_cb; ccfg.snapshot_cb = cloud_snapshot_cb; ccfg.storage_lock = storage_mutex;
    
    setenv("TZ", "IST-5:30", 1);
    tzset();

    cloud_client_init(&ccfg);
    BaseType_t sched_ok = xTaskCreate(schedule_task, "scheduler", 4096, NULL, 3, &schedule_task_handle);
    if (sched_ok != pdPASS) ESP_LOGE(TAG, "Schedule task creation failed");

    ESP_LOGI(TAG, "========================================");
    ESP_LOGI(TAG, "Offline Smart Home ready");
    ESP_LOGI(TAG, "Control:  http://%s/", AP_IP_ADDR);
    ESP_LOGI(TAG, "AP only: no STA, no Internet");
    ESP_LOGI(TAG, "Relays: 7 total (5 physical switch inputs + 2 web-only)");
    ESP_LOGI(TAG, "========================================");

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
}
