package example.java.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/20
 **/
public class FlinkKafkaDDLDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment, environmentSettings);

        String createTable = "create table wordCount(name varchar,score bigint) " +
                "WITH('connector.type' = 'kafka', " +
                "'connector.version' = '0.10'," +
                "'connector.topic' = 'flink', " +
                "'connector.properties.0.key' = 'bootstrap.servers'," +
                "'connector.properties.0.value' = 'localhost:9092'," +
                "'connector.properties.1.key' = 'zookeeper.connect'," +
                "'connector.properties.1.value' = 'localhost:2181'," +
                "'format.derive-schema' = 'true'," +
                "'update-mode' = 'append'," +
                "'format.type' = 'json')";

        tableEnv.sqlUpdate(createTable);
        for (String t:tableEnv.listTables()){
            System.out.println(t);
        }
        String query = "select name,sum(score) as score from wordCount group by name";

        Table sqlQuery = tableEnv.sqlQuery(query);

        tableEnv.toRetractStream(sqlQuery, Row.class).print();
//
        tableEnv.execute("table env");

    }

}
