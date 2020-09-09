package com.lhs.flink.rule;

import com.lhs.flink.rule.pojo.RedisDataWithName;
import com.lhs.flink.rule.process.DataProcessWithRedisConfig;
import com.lhs.flink.rule.process.JsonLogFilter;
import com.lhs.flink.rule.sink.RedisSinkWithPools;
import com.lhs.flink.rule.sources.LogConfigSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author lihuasong
 * @description
 *      描述：在不停止当前flink的情况下，添加新的任务
 * @create 2020/5/21
 *
 *  flink run -m yarn-cluster -yjm 2048 -yn 4 -ys 2 -ytm 2048 -ynm rule_jobs -p 8 -sae \
 *  -c com.xianlai.streaming.single.StreamingEntry streaming-single.jar \
 *  -kafka.consume.servers 10.111.73.209:9092,10.111.73.210:9092,10.111.73.211:9092 \
 *  -kafka.group single_job_group_in_test \
 *  -kafka.partition.discover.interval.ms 30000 \
 *  -checkpoint.duration.ms 60000 \
 *  -config.sleep.ms 5000 \
 *  -jedis.sync.size 100
 *
 *  -kafka.consume.servers 10.111.73.209:9092,10.111.73.210:9092,10.111.73.211:9092 -path.checkpoint hdfs:///user/hadoop/flink/state/checkpoints -kafka.group single_job_group_in_test -kafka.partition.discover.interval.ms 30000 -checkpoint.duration.ms 60000 -config.sleep.ms 5000 -jedis.sync.size 150
 **/
public class StreamingEntry {

    /**
     * kafka consumer 的模糊匹配
     */
    private static Pattern ConsumerTopicPatterns = Pattern.compile("^(bdt)((?!client_action).)*$");

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        for (Map.Entry<String, String> en : parameterTool.toMap().entrySet()) {
            System.out.println("key:[" + en.getKey() + "] value[" + en.getValue() + "]");
        }
        // 消费kafka的地址
        String kafkaConsumeServers = parameterTool.get("kafka.consume.servers","10.122.238.97:9092");
        // 消费者的group.id
        String groupId = parameterTool.get("kafka.group","wash_group");
        // 广播配置的间隔时间
        long sleepMs = parameterTool.getLong("config.sleep.ms", 2000L);
        // checkpoint的间隔
        long checkpointDurationMs = parameterTool.getLong("checkpoint.duration.ms",30000L);
        // 分区发现的间隔时间,毫秒
        String kafkaPartitionDiscoveryIntervalMS = parameterTool.get("kafka.partition.discover.interval.ms","30000");

        // kafka消费者的配置
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers",kafkaConsumeServers);
        consumerConfig.put("group.id",groupId);
        // 消费者支持动态发现分区
        consumerConfig.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, kafkaPartitionDiscoveryIntervalMS);


        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        FsStateBackend fsStateBackend = new FsStateBackend(parameterTool.get("path.checkpoint","hdfs://10.122.238.97:9000/user/hadoop/flink/checkpoint/single"));

        environment.setStateBackend(fsStateBackend);
        environment.enableCheckpointing(checkpointDurationMs);
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.seconds(60),
                Time.seconds(10)
        ));

        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<>(ConsumerTopicPatterns,new SimpleStringSchema(),consumerConfig);
        kafkaSource.setCommitOffsetsOnCheckpoints(true);

        DataStream<String> dataStreamSource = environment
                .addSource(kafkaSource,"kafka_source")
                .rebalance()
                .filter(new JsonLogFilter())
                .name("json_filter");

        // 广播变量
        MapStateDescriptor<String,Map<String,String>> logConfigState = new MapStateDescriptor<String, Map<String,String>>(
                "logProcessState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Map<String,String>>() {})
        );

        // 广播变量的数据源
        DataStreamSource<Map<String, String>> logConfigSource = environment.addSource(new LogConfigSource(sleepMs),"config_source");

        // 将上述配置广播到每个task
        BroadcastStream<Map<String, String>> broadcastLogConfig = logConfigSource.broadcast(logConfigState);

        // 原始数据接收广播配置，根据配置处理原始数据
        SingleOutputStreamOperator<RedisDataWithName> redisDataSingleOutputStreamOperator = dataStreamSource.connect(broadcastLogConfig).process(new DataProcessWithRedisConfig()).name("process_with_config");

        //将数据写到 redis
        redisDataSingleOutputStreamOperator.addSink(new RedisSinkWithPools(parameterTool)).name("redis_sinks");
        environment.execute("single-job");

    }

}
