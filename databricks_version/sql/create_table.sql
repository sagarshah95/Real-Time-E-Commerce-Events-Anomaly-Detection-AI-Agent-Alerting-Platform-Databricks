Create table if not exists `{CATALOG}`.`{SCHEMA}`.`alert-history`(
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    alert_send_at TIMESTAMP,
    alert_reasons array<string>,
    alert_text string

)