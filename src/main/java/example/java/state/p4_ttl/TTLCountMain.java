package example.java.state.p4_ttl;

import example.java.state.sink.RedisSinkInstance2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/8/7
 **/
public class TTLCountMain {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //web地址 http://localhost:8081/#/overview
        StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.enableCheckpointing(2000);

        FsStateBackend fsStateBackend = new FsStateBackend("file:///checkpoint_p4", false);

        env.setStateBackend(fsStateBackend);

        // kafka需要的最低配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","p4_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("topic_2_partitions", new SimpleStringSchema(),properties);

        DataStream<String> stream = env.addSource(source);

        SingleOutputStreamOperator<Tuple2<String, Long>> mapData = stream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                return new Tuple2<>(s, 1L);
            }
        });

        mapData.keyBy(0)
                .flatMap(new CountSateTTL())
                .print();


        env.execute("state count");
    }
}
