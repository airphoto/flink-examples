package com.lhs.flink;

import com.lhs.flink.process.DataWashWithConfig;
import com.lhs.flink.serialization.KafkaKeyedSerialization;
import com.lhs.flink.sources.LogConfigSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/21
 *
 *  flink run -m yarn-cluster -yjm 2048 -yn 4 -ys 2 -ytm 2048 -ynm wash_data -p 8 -sae \
 *  -c com.lhs.flink.WashEntry wash-data-1.0-SNAPSHOT.jar \
 *  -kafka.consume.servers 10.122.238.97:9092 \
 *  -kafka.produce.servers 10.122.238.97:9092 \
 *  -kafka.partition.discover.interval.ms 30000 \
 *  -kafka.group wash_group \
 *  -config.sleep.ms 5000 \
 *  -metric.map.ttl 30 \
 *  -kafka.sink.default.topic wash \
 *  -kafka.sink.error.topic error
 *
 *
 **/
public class WashEntry {

    /**
     * kafka consumer 的模糊匹配
     */
    private static Pattern ConsumerTopicPatterns = Pattern.compile("^(bdt)\\w*");

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        for (Map.Entry<String, String> en : parameterTool.toMap().entrySet()) {
            System.out.println("key:[" + en.getKey() + "] value[" + en.getValue() + "]");
        }
        // 消费kafka的地址
        String kafkaConsumeServers = parameterTool.get("kafka.consume.servers","10.122.238.97:9092");
        // 生产kafka的地址
        String kafkaProduceServers = parameterTool.get("kafka.produce.servers","10.122.238.97:9092");
        // 消费者的group.id
        String groupId = parameterTool.get("kafka.group","wash_group");
        // 广播配置的间隔时间
        long sleepMs = parameterTool.getLong("config.sleep.ms", 2000L);
        // 分区发现的间隔时间,毫秒
        String kafkaPartitionDiscoveryIntervalMS = parameterTool.get("kafka.partition.discover.interval.ms","30000");


        // kafka消费者的配置
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers",kafkaConsumeServers);
        consumerConfig.put("group.id",groupId);
        // 消费者支持动态发现分区
        consumerConfig.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, kafkaPartitionDiscoveryIntervalMS);


        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10000);

        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<>(ConsumerTopicPatterns,new SimpleStringSchema(),consumerConfig);

        DataStream<String> dataStreamSource = environment.addSource(kafkaSource).rebalance();

        // 广播变量
        MapStateDescriptor<String,Map<String,String>> logConfigState = new MapStateDescriptor<String, Map<String,String>>(
                "logConfigState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Map<String,String>>() {})
        );

        // 广播变量的数据源
        DataStreamSource<Map<String, String>> logConfigSource = environment.addSource(new LogConfigSource(sleepMs));

        // 将上述配置广播到每个task
        BroadcastStream<Map<String, String>> broadcastLogConfig = logConfigSource.broadcast(logConfigState);

        // 原始数据接收广播配置，根据配置处理原始数据
        SingleOutputStreamOperator<Tuple2<String, String>> washData = dataStreamSource.connect(broadcastLogConfig).process(new DataWashWithConfig(parameterTool));

//         原始数据生成Tuple2<Topic,Log> 的形式，需要自定义序列化的方式
        KafkaKeyedSerialization kafkaKeyedSerialization = new KafkaKeyedSerialization();

//         生产者
        FlinkKafkaProducer010<Tuple2<String, String>> flinkKafkaProducer = new FlinkKafkaProducer010<>(kafkaProduceServers, "", kafkaKeyedSerialization);

//         将数据写到kafka
        washData.addSink(flinkKafkaProducer);

        environment.execute();

    }

}
