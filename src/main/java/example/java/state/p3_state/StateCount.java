package example.java.state.p3_state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/8/7
 **/
public class StateCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        String topic = "test";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","flink_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<>(topic,new SimpleStringSchema(),properties);

        DataStream<String> stream = env.addSource(source);

        DataStream<Tuple2<Long,Long>> dataStream2 = stream.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String s) throws Exception {
                return new Tuple2<>(Long.parseLong(s),1L);
            }
        });


        dataStream2.keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();



        env.execute("");
    }
}
