package com.lhs.flink.minibatch;

import com.lhs.flink.sources.CountConfigSource;
import com.lhs.flink.processes.DataKeyPoint;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/25
 **/
public class MiniBatchCountMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id","broadcast");

        List<String> topics = new ArrayList<>();
        topics.add("");

        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<>(topics,new SimpleStringSchema(),properties);

        DataStreamSource<String> dataStreamSource = environment.addSource(kafkaSource);

        MapStateDescriptor<String,Map<String,String>> countConfigState = new MapStateDescriptor<String, Map<String,String>>(
                "CountConfigState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Map<String,String>>() {})
        );

        DataStreamSource<Map<String, Map<String, String>>> countConfigSource = environment.addSource(new CountConfigSource());

        BroadcastStream<Map<String, Map<String, String>>> broadlcastCountConfig = countConfigSource.broadcast(countConfigState);

        dataStreamSource.connect(broadlcastCountConfig)
                .process(new DataKeyPoint())
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)));


        environment.execute();
    }

}
