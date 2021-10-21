package joining;

import joining.pojo.JoinStream;
import joining.pojo.Stream1;
import joining.pojo.Stream2;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @ClassName JoiningDemo
 * @Author lihuasong
 * @Description
 * @Date 2021-10-18 20:17
 * @Version V1.0
 **/

public class JoiningDemo {

    public static void main(String[] args) throws Exception {

        // flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // kafka source
        Properties kafkaProp1 = new Properties();
        kafkaProp1.setProperty("bootstrap.servers","10.122.238.97:9092");
        kafkaProp1.setProperty("group.id","flink_join_demo");

        FlinkKafkaConsumer010<String> source1 = new FlinkKafkaConsumer010<String>("link_bigdata100", new SimpleStringSchema(),kafkaProp1);

        SingleOutputStreamOperator<Stream1> op1 = env.addSource(source1).map(x -> {
            String[] fields = x.split(",");
            return new Stream1(fields[0], fields[1]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Stream1>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Stream1 stream1) {
                return System.currentTimeMillis();
            }
        });

        // kafka source
        Properties kafkaProp2 = new Properties();
        kafkaProp2.setProperty("bootstrap.servers","10.122.238.97:9092");
        kafkaProp2.setProperty("group.id","flink_join_demo");

        FlinkKafkaConsumer010<String> source2 = new FlinkKafkaConsumer010<String>("topic_2_partitions", new SimpleStringSchema(),kafkaProp2);
        SingleOutputStreamOperator<Stream2> op2 = env.addSource(source2).map(x -> {
            String[] fields = x.split(",");
            return new Stream2(fields[0], fields[2]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Stream2>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Stream2 stream2) {
                return System.currentTimeMillis();
            }
        });

        DataStream<JoinStream> join = op1.join(op2)
                .where(stream1 -> stream1.id)
                .equalTo(stream2 -> stream2.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Stream1, Stream2, JoinStream>() {
                    @Override
                    public JoinStream join(Stream1 stream1, Stream2 stream2) throws Exception {
                        return new JoinStream(stream1.name,stream2.value);
                    }
                });

        join.print();

        env.execute("tumbling join");
    }
}
