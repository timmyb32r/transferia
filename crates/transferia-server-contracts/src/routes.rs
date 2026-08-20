use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ApiRoute {
    pub name: &'static str,
    pub method: &'static str,
    pub path: &'static str,
    pub response: &'static str,
}

macro_rules! routes {
    ($(($constant:ident, $name:literal, $method:literal, $path:literal, $response:literal)),+ $(,)?) => {
        $(pub const $constant: ApiRoute = ApiRoute { name: $name, method: $method, path: $path, response: $response };)+
        pub const API_ROUTES: &[ApiRoute] = &[$($constant),+];
    };
}

routes![
    (HEALTH, "health", "GET", "/api/v1/health", "health_response"),
    (
        CATALOG,
        "catalog",
        "GET",
        "/api/v1/catalog",
        "catalog_response"
    ),
    (
        OPTIONS,
        "options",
        "POST",
        "/api/v1/options/{key}",
        "dynamic_options_response"
    ),
    (
        CHECK_CONNECTION,
        "check_connection",
        "POST",
        "/api/v1/check-connection",
        "connection_check_response"
    ),
    (
        PREVIEW_MESSAGE,
        "preview_message",
        "POST",
        "/api/v1/preview-message",
        "message_preview_response"
    ),
    (
        SQL_PLAYGROUND,
        "sql_playground",
        "POST",
        "/api/v1/playground/sql",
        "sql_playground_response"
    ),
    (
        RENDER_YAML,
        "render_yaml",
        "POST",
        "/api/v1/config/yaml",
        "yaml_response"
    ),
    (
        PARSE_YAML,
        "parse_yaml",
        "POST",
        "/api/v1/config/from-yaml",
        "config_response"
    ),
    (
        DISCOVER,
        "discover",
        "POST",
        "/api/v1/discover",
        "discovery_response"
    ),
    (
        LIST_DELIVERIES,
        "list_deliveries",
        "GET",
        "/api/v1/deliveries",
        "delivery_list_response"
    ),
    (
        CREATE_DELIVERY,
        "create_delivery",
        "POST",
        "/api/v1/deliveries",
        "delivery_response"
    ),
    (
        GET_DELIVERY,
        "get_delivery",
        "GET",
        "/api/v1/deliveries/{id}",
        "delivery_response"
    ),
    (
        UPDATE_DELIVERY,
        "update_delivery",
        "PUT",
        "/api/v1/deliveries/{id}",
        "delivery_response"
    ),
    (
        DELETE_DELIVERY,
        "delete_delivery",
        "DELETE",
        "/api/v1/deliveries/{id}",
        "delivery_response"
    ),
    (
        VALIDATE,
        "validate",
        "POST",
        "/api/v1/deliveries/{id}/validate",
        "validation_response"
    ),
    (
        ACTIVATE,
        "activate",
        "POST",
        "/api/v1/deliveries/{id}/activate",
        "delivery_response"
    ),
    (
        STOP,
        "stop",
        "POST",
        "/api/v1/deliveries/{id}/stop",
        "delivery_response"
    ),
    (
        WORKER_LOGS,
        "worker_logs",
        "GET",
        "/api/v1/deliveries/{id}/logs",
        "worker_logs_response"
    ),
    (
        WORKER_LOG,
        "worker_log",
        "GET",
        "/api/v1/deliveries/{id}/logs/{worker_id}",
        "worker_log_response"
    ),
];
