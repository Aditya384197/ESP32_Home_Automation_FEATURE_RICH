idf_component_register(
    SRCS "main.c" "cloud_client.c"
    INCLUDE_DIRS "."
    REQUIRES
        esp_system
        esp_wifi
        esp_netif
        esp_event
        esp_http_server
        esp_http_client
        esp-tls
        driver
        nvs_flash
        json
        mdns
        mqtt
)
