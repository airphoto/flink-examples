package com.lhs.flink.serialization;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @ClassNameKafkaKeyedSink
 * @Description
 * @Author lihuasong
 * @Date2020/5/31 16:22
 * @Version V1.0
 **/
public class KafkaKeyedSerialization implements KeyedSerializationSchema<Tuple2<String,String>> {
    @Override
    public byte[] serializeKey(Tuple2<String, String> stringStringTuple2) {
        return stringStringTuple2.f0.getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public byte[] serializeValue(Tuple2<String, String> stringStringTuple2) {
        return stringStringTuple2.f1.getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public String getTargetTopic(Tuple2<String, String> stringStringTuple2) {
        return stringStringTuple2.f0;
    }
}
