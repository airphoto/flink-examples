package example.java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @ClassNameMyKafkaProducer
 * @Description
 * @Author lihuasong
 * @Date2020/4/19 10:51
 * @Version V1.0
 **/
public class MyKafkaProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers","10.122.238.97:9092");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(properties);

        int i = 0;
        while (true){
            long current = System.currentTimeMillis();
            Random random = new Random(30);
            String str = "{\"type\":\"gameover\",\"appId\":"+random.nextInt()+",\"userId\":85"+random.nextInt()+",\"time\":\""+current+"\",\"properties\":{\"playId\":"+random.nextInt()+"}}";
            ProducerRecord<String,String> record1 = new ProducerRecord<>("bdt_original",str);
            producer.send(record1);
            i++;
            System.out.println(i+"->"+str);
            Thread.sleep(1000L);
        }

    }

}
