-- 开启 mini-batch
SET table.exec.mini-batch.enabled=true;
-- mini-batch的时间间隔，即作业需要额外忍受的延迟
SET table.exec.mini-batch.allow-latency=10s;
-- 一个 mini-batch 中允许最多缓存的数据
SET table.exec.mini-batch.size=1000;
-- 开启 local-global 优化
SET table.optimizer.agg-phase-strategy=TWO_PHASE;
-- 开启 distinct agg 切分
SET table.optimizer.distinct-agg.split.enabled=true;

-- source （使用Flink DDL去创建并连接Kafka中的topic）
CREATE TABLE json_logs (
    `key` VARCHAR,
    `field` VARCHAR,
    `value` VARCHAR,
    `time` VARCHAR
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
    'format.json-schema' = '{"type":"object","properties":{"key":{"type":"string"},"field":{"type":"string"},"value":{"type":"string"},"time":{"type":"string"}}}'
);

-- sink（使用Flink DDL连接MySQL结果表）
CREATE TABLE reg_data_77781 (
    target_day VARCHAR,
    m_hour VARCHAR,
    m_minute VARCHAR,
    regs BIGINT
) WITH (
    'connector.type' = 'print'
);

-- Group Aggregation
INSERT INTO reg_data_77781
SELECT
  target_day,m_hour,m_minute,sum(regs) as regs
  from
  (select from_unixtime(cast(substring(`time`,0,10) as int),'yyyy-MM-dd') as target_day,
  from_unixtime(cast(substring(`time`,0,10) as int),'HH') as m_hour,
  from_unixtime(cast(substring(`time`,0,10) as int),'mm') as m_minute,
  1 as regs
  from json_logs
  ) group by target_day,m_hour,m_minute;
