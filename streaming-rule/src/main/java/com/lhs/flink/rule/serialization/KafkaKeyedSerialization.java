package com.lhs.flink.rule.serialization;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.nio.charset.Charset;

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
