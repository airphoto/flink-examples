package com.lhs.flink.jobs.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaWordCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //web地址 http://localhost:8081/#/overview
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // kafka需要的最低配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","10.51.237.123:9092");
        properties.setProperty("group.id","flink_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("zhining", new SimpleStringSchema(),properties);

        DataStream<String> stream = env.addSource(source);

        DataStream<Tuple2<String,Integer>> flatMapData = stream.flatMap((String line, Collector< Tuple2<String,Integer>> out) ->{
            for (String field : line.split(" ")){
                out.collect(new Tuple2<String,Integer>(field,1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT));

        SingleOutputStreamOperator<Tuple2<String,Integer>> result =  flatMapData
                .keyBy(0)
                .timeWindow(Time.seconds(5)) // 窗口大小
                .sum(1);

        result.print();

        env.execute("kafka stream word count");

    }
}
