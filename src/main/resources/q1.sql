-- 开启 mini-batch
SET table.exec.mini-batch.enabled=true;
-- mini-batch的时间间隔，即作业需要额外忍受的延迟
SET table.exec.mini-batch.allow-latency=1s;
-- 一个 mini-batch 中允许最多缓存的数据
SET table.exec.mini-batch.size=1000;
-- 开启 local-global 优化
SET table.optimizer.agg-phase-strategy=TWO_PHASE;
-- 开启 distinct agg 切分
SET table.optimizer.distinct-agg.split.enabled=true;

-- source （使用Flink DDL去创建并连接Kafka中的topic）
CREATE TABLE user_log (
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP(3)
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
    'format.type' = 'json',
    'format.fail-on-missing-field' = 'false',
    'format.json-schema' = '{"type":"object","properties":{"user_id":{"type":"string"},"item_id":{"type":"string"},"category_id":{"type":"string"},"behavior":{"type":"string"},"ts":{"type":"string","format":"date-time"}}}'
);

-- sink（使用Flink DDL连接MySQL结果表）
CREATE TABLE pvuv_sink (
    dt VARCHAR,
    pv BIGINT,
    uv BIGINT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://127.0.01:3306/flink_test',
    'connector.table' = 'pvuv_sink',
    'connector.username' = 'root',
    'connector.password' = '123456',
    'connector.write.flush.max-rows' = '1'
);

-- Group Aggregation
INSERT INTO pvuv_sink
SELECT
  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,
  COUNT(*) AS pv,
  COUNT(DISTINCT user_id) AS uv
FROM user_log
WHERE ts IS NOT NULL
GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00');
