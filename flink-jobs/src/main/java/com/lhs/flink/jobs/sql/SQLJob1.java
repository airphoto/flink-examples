package com.lhs.flink.jobs.sql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;


public class SQLJob1 {

    public static void main(String[] args) throws Exception {
        String sql = "CREATE TABLE until_canal_sink (`db` VARCHAR,`tb` VARCHAR,`target_day` VARCHAR,`target_minute` VARCHAR,`lines` BIGINT) WITH ('connector.type' = 'jdbc','connector.url' = 'jdbc:mysql://10.122.238.97:13306/datacenter','connector.table' = 'flink_until_canal_sink','connector.username' = 'xl_test','connector.password' = 'xianlai@test2018','connector.write.flush.max-rows' = '1')";
        SqlParser.create(sql,
                SqlParser.configBuilder()
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setQuoting(Quoting.BACK_TICK)
                .setUnquotedCasing(Casing.TO_UPPER)
                .setQuotedCasing(Casing.UNCHANGED)
                .setConformance(FlinkSqlConformance.DEFAULT)
                .build()
        ).parseStmtList();

    }

}
