package example.java.sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


/**
 * @ClassNameStreamingTableApp
 * @Description
 * @Author lihuasong
 * @Date2020/4/20 22:01
 * @Version V1.0
 **/
public class StreamingTableApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        Kafka kafkaConnector = new Kafka()
                .version("0.10")
                .topic("flink")
                .property("bootstrap.servers","localhost:9092")
                .property("zookeeper.connect","localhost:2181")
                .property("group.id","flink-table");

        tableEnvironment.connect(kafkaConnector)
                .withFormat(
                        new Json().failOnMissingField(true)
                ).withSchema(
                new Schema()
                        .field("appid", "int")
                        .field("userid", "int")
                        .field("view", "int")
        ).inAppendMode()
                .createTemporaryTable("user_table");

        environment.setRestartStrategy(RestartStrategies.failureRateRestart(1, Time.seconds(1), Time.seconds(1)));

        Table table = tableEnvironment.from("user_table");

        Table result = table.groupBy("appid,userid")
                .select("appid,userid,view.sum as view_sum");

        DataStream<Tuple2<Boolean,Row>> stream = tableEnvironment.toRetractStream(result,Row.class);

//        stream.addSink(new PrintSinkFunction<>(false));
        stream.print();
        tableEnvironment.execute("streaming-table");
    }
}
