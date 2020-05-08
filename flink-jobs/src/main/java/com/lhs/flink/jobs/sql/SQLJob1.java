package com.lhs.flink.jobs.sql;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;


public class SQLJob1 {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.getConfig().setIdleStateRetentionTime(Time.seconds(10),Time.seconds(320));

        tableEnvironment.sqlUpdate("CREATE TABLE user_log ( user_id VARCHAR, item_id VARCHAR, category_id VARCHAR, behavior VARCHAR, ts TIMESTAMP(3) ) WITH ( 'connector.type' = 'kafka', 'connector.version' = '0.10', 'connector.topic' = 'flink', 'connector.startup-mode' = 'latest-offset', 'connector.properties.0.key' = 'zookeeper.connect', 'connector.properties.0.value' = 'localhost:2181', 'connector.properties.1.key' = 'bootstrap.servers', 'connector.properties.1.value' = 'localhost:9092', 'update-mode' = 'append', 'format.type' = 'json', 'format.fail-on-missing-field' = 'false', 'format.json-schema' = '{\"type\":\"object\",\"properties\":{\"user_id\":{\"type\":\"string\"},\"item_id\":{\"type\":\"string\"},\"category_id\":{\"type\":\"string\"},\"behavior\":{\"type\":\"string\"},\"ts\":{\"type\":\"string\",\"format\":\"date-time\"}}}' )");

        tableEnvironment.sqlUpdate("CREATE TABLE pvuv_sink ( dt VARCHAR, pv BIGINT, uv BIGINT ) WITH ( 'connector.type' = 'print')");

        tableEnvironment.sqlUpdate("INSERT INTO pvuv_sink SELECT DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt, COUNT(*) AS pv, COUNT(DISTINCT user_id) AS uv FROM user_log WHERE ts IS NOT NULL GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')");

        tableEnvironment.execute("query");
    }

}
