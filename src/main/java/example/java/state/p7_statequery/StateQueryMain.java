package example.java.state.p7_statequery;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * Created by abel on 2020/4/18.
 */
public class StateQueryMain {
    public static void main(String[] args) throws Exception {
//        Configuration configuration = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.enableCheckpointing(2000);

        FsStateBackend fsStateBackend = new FsStateBackend("file:///checkpoint_p7");
        environment.setStateBackend(fsStateBackend);

        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","p7_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<>("topic_2_partitions",new SimpleStringSchema(),properties);

        DataStream<String> stream = environment.addSource(source);

        SingleOutputStreamOperator<Tuple2<String,Long>> mapData = stream.map((String str)->{
            return new Tuple2<>(str,1L);
        }).returns(new TypeHint<Tuple2<String, Long>>() {});



        mapData.keyBy(0)
                   .map(new QueryStateMapFunc())
                   .print();

        environment.execute("exe");

    }
}
