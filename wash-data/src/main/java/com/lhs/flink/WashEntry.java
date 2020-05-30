package com.lhs.flink;

import com.lhs.flink.pojo.LogConfig;
import com.lhs.flink.process.DataWashWithConfig;
import com.lhs.flink.sources.LogConfigSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.*;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/21
 **/
public class WashEntry {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String kafkaConsumeServers = parameterTool.get("kafka.consume.servers","localhost:9092");
        String kafkaProduceServers = parameterTool.get("kafka.produce.servers","localhost:9092");
        String groupId = parameterTool.get("kafka.group","wash_group");
        String[] consumeTopics = parameterTool.get("kafka.consume.topics", "original").split(",");
        String producerTopic = parameterTool.get("kafka.produce.topic", "wash");
        long sleepMs = parameterTool.getLong("config.sleep.ms", 2000L);

        Configuration configuration = new Configuration();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        Properties properties = new Properties();
        properties.put("bootstrap.servers",kafkaConsumeServers);
        properties.put("group.id",groupId);

        List<String> topics = Arrays.asList(consumeTopics);

        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<>(topics,new SimpleStringSchema(),properties);

        DataStreamSource<String> dataStreamSource = environment.addSource(kafkaSource);

        MapStateDescriptor<String,Map<String,String>> logConfigState = new MapStateDescriptor<String, Map<String,String>>(
                "logConfigState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Map<String,String>>() {})
        );

        DataStreamSource<Map<String, String>> logConfigSource = environment.addSource(new LogConfigSource(sleepMs));

        BroadcastStream<Map<String, String>> broadcastLogConfig = logConfigSource.broadcast(logConfigState);

        SingleOutputStreamOperator<String> washData = dataStreamSource.connect(broadcastLogConfig)
                .process(new DataWashWithConfig());

        FlinkKafkaProducer010<String> kafkaProducer010 = new FlinkKafkaProducer010<>(kafkaProduceServers, producerTopic, new SimpleStringSchema());

        washData.addSink(kafkaProducer010);

        environment.execute();

    }

}
