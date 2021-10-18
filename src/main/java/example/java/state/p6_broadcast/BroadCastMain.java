package example.java.state.p6_broadcast;

import com.lhs.flink.example.java.state.pojo.JobConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Map;
import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/15
 **/
public class BroadCastMain {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment environment = LocalStreamEnvironment.createLocalEnvironmentWithWebUI(configuration);

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id","broadcast");

        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<String>("canal",new SimpleStringSchema(),properties);

        DataStreamSource<String> stream = environment.addSource(kafkaSource);

        MapStateDescriptor<String, JobConfig> broadcastState = new MapStateDescriptor<>(
                "BroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<JobConfig>() {
                })
        );

        // 自定义source去连接外部数据源的配置文件
        DataStreamSource<Map<String, JobConfig>> mapDataStreamSource = environment.addSource(new GetJobConfig());

        // 广播配置文件
        BroadcastStream<Map<String, JobConfig>> mapBroadcastStream = mapDataStreamSource.broadcast(broadcastState);

//        // 数据源去连接配置文件然后根据配置文件做修改
//        stream.connect(mapBroadcastStream)
//                .process(new MyBroadcastProcessFunc())
//                .print();

        stream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                return new Tuple2<>(s,1L);
            }
        }).keyBy(0)
                .connect(mapBroadcastStream)
                .process(new MyKeydBroadcastProcessFunc())
                .print();

        environment.execute("broad-cast");

    }
}
