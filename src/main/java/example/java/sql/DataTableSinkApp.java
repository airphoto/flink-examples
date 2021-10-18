package example.java.sql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.example.java.sql.p1_sources_sinks.redis.connector.RedisConnector;
import com.lhs.flink.example.java.sql.pojo.RedisCase;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;

import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/22
 **/
public class DataTableSinkApp {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.connect(
                new RedisConnector("redis",1,false)
        ).inAppendMode()
                .withSchema(
                        new Schema()
                        .field("s", DataTypes.STRING())
                        .field("f", DataTypes.STRING())
                        .field("v", DataTypes.STRING())
                ).createTemporaryTable("redis_sink");


        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id","data_table");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("flink",new SimpleStringSchema(),properties);

        DataStreamSource<String> stringDataStreamSource = environment.addSource(source);
        SingleOutputStreamOperator<RedisCase> streamOperator = stringDataStreamSource.map((str) -> {
            JSONObject object = JSON.parseObject(str);
            String key = object.getString("key");
            String field = object.getString("field");
            String value = object.getString("value");
            return new RedisCase(key, field, value);
        }).returns(TypeInformation.of(new TypeHint<RedisCase>() {}));

        Table table = tableEnvironment.fromDataStream(streamOperator);
        tableEnvironment.createTemporaryView("redis_table",table);


//        RedisAppendStreamTableSink redisRetractStreamTableSink = new RedisAppendStreamTableSink();
//
//        System.out.println(redisRetractStreamTableSink.getTableSchema());

        tableEnvironment.sqlUpdate("insert into redis_sink select k,f,v from redis_table");


//        tableEnvironment.toAppendStream(sqlQuery, RedisCase.class).print();

        tableEnvironment.execute("table sink");

    }

}
