package example.java.state.p1_wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author lihuasong
 * @description
 *
 *      Kafka Word count 的基础配置
 *
 * @create 2019/7/31
 **/
public class KafkaWordCount {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",params.get("kafka_servers"));
        properties.setProperty("group.id",params.get("flink_group"));
        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>(params.get("kafka_topics","test"),new SimpleStringSchema(),properties);

        DataStream<String> events = env.addSource(source);

        DataStream<Tuple2<String,Integer>> count = events
                .flatMap(new flatMapFun())
                .keyBy(0)
                .sum(1);

        count.print();

        env.execute("flink kafka word count");
    }

    public static final class flatMapFun implements FlatMapFunction<String,Tuple2<String,Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : s.split(" ")){
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
