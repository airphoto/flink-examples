package async_io;

import async_io.pojo.CategoryInfo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName AsyncIODemo
 * @Author lihuasong
 * @Description
 *      主函数
 * @Date 2021-09-30 15:00
 * @Version V1.0
 **/

public class AsyncIODemo {
    public static void main(String[] args) throws Exception {
        // flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // kafka source
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers","10.122.238.97:9092");
        kafkaProp.setProperty("group.id","flink_async_io_demo");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("topic_2_partitions", new SimpleStringSchema(),kafkaProp);

        SingleOutputStreamOperator<CategoryInfo> categoryInfoStrea = env.addSource(source).map(x -> new CategoryInfo(x, null));

        // unorderedWait 无序等待
        SingleOutputStreamOperator<CategoryInfo> result1 = AsyncDataStream.unorderedWait(categoryInfoStrea, new AsyncFunction1(), 1000, TimeUnit.SECONDS, 10);

        result1.print("方式一：Java-vertx中提供的异步client实现异步的io \\n");

        env.execute("flink async io demo");
    }
}
