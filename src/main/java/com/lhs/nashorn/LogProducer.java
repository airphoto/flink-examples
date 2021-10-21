package com.lhs.nashorn;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/7/12
 **/
public class LogProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String topic = "topic_2_partitions"; //消息所属的 Topic，请在控制台申请之后，填写在这里

        while (true){
            String datap8=System.currentTimeMillis()+",word,1";
//            String datap2="word,"+System.currentTimeMillis();
            String datap4="word";
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, datap8);

            try {
                producer.send(data);
                System.out.println(data.value());
                Thread.sleep(1000);
            } catch (Exception e) {
                // 要考虑重试
                e.printStackTrace();
            }finally {
                producer.flush();
            }
        }
    }
}