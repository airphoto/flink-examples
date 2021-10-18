-- source （使用Flink DDL去创建并连接Kafka中的topic）
CREATE TABLE user_log (
    k VARCHAR,
    f VARCHAR,
    v VARCHAR
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = '0.10',
    'connector.topic' = 'flink',
    'connector.startup-mode' = 'latest-offset',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'update-mode' = 'append',
    'format.type' = 'log_parser',
    'format.fail-on-missing-field' = 'false',
    'format.json-schema' = '{"type":"object","properties":{"k":{"type":"string"},"f":{"type":"string"},"v":{"type":"string"}}}'
);

-- sink（使用Flink DDL连接MySQL结果表）
CREATE TABLE pvuv_sink (
    k VARCHAR,
    f VARCHAR,
    v VARCHAR
) WITH (
    'connector.type' = 'redis'
);

-- Group Aggregation
INSERT INTO pvuv_sink
SELECT
  k,
  f,
  v
FROM user_log
