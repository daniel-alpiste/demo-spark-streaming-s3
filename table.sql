CREATE TABLE IF NOT EXISTS db_demo.json_events_delta (
    event_time STRING,
    user_id STRING,
    action STRING,
    value DOUBLE
)
USING DELTA
LOCATION 'dbfs:/mnt/path_to_delta_table/json_events_delta';