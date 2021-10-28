package example.java.cep.examples.p2_doors;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName DoorsEntry
 * @Author lihuasong
 * @Description
 * @Date 2021-10-27 18:35
 * @Version V1.0
 **/

public class DoorsEntry {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000L);

        // 不使用pojo的时间？？？  IngestionTimeExtractor， 这个是当指定时间特性为IngestionTime时，直接生成时间戳和获取水印。
        AssignerWithPeriodicWatermarks<AccessEvent> extractor = new IngestionTimeExtractor<AccessEvent>();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","cep_doors_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("topic_2_partitions",new SimpleStringSchema(),properties);

        DataStream<AccessEvent> stream = env.addSource(source).map(x->{
            String[] fields = x.split(",");
            return new AccessEvent(Integer.parseInt(fields[0]),Integer.parseInt(fields[1]),fields[2],Integer.parseInt(fields[3]),fields[4],fields[5]);
        }).assignTimestampsAndWatermarks(extractor);

        // 根据员工id进行数据分组
        KeyedStream<AccessEvent, String> keyedStream = stream.keyBy(AccessEvent::getEmployeeSysNo);

        // 匹配模式，此处10秒是方便测试
        Pattern<AccessEvent, AccessEvent> pattern1 = Pattern.<AccessEvent>begin("outdoor")
                .where(new SimpleCondition<AccessEvent>() {
                    @Override
                    public boolean filter(AccessEvent accessEvent) throws Exception {
                        return accessEvent.getDoorStatus().equals("out");
                    }
                }).next("indoor").where(new SimpleCondition<AccessEvent>() {
                    @Override
                    public boolean filter(AccessEvent accessEvent) throws Exception {
                        return accessEvent.getDoorStatus().equals("in");
                    }
                }).within(Time.seconds(10)).times(1);

        Pattern<AccessEvent, AccessEvent> pattern2 = Pattern.<AccessEvent>begin("out")
                .where(new SimpleCondition<AccessEvent>() {
                    @Override
                    public boolean filter(AccessEvent accessEvent) throws Exception {
                        return accessEvent.getDoorStatus().equals("out");
                    }
                }).next("in").where(new SimpleCondition<AccessEvent>() {
                    @Override
                    public boolean filter(AccessEvent accessEvent) throws Exception {
                        return accessEvent.getDoorStatus().equals("in");
                    }
                }).times(3).within(Time.seconds(60));


        PatternStream<AccessEvent> patternStream = CEP.pattern(keyedStream, pattern1);
        PatternStream<AccessEvent> patternStreamTimes = CEP.pattern(keyedStream, pattern2);

        patternStreamTimes.process(new PatternProcessFunction<AccessEvent,String>(){
            @Override
            public void processMatch(Map<String, List<AccessEvent>> map, Context context, Collector<String> collector) throws Exception {
                System.out.println("频繁出入");
                collector.collect(JSON.toJSONString(map));
            }
        }).print();


        SingleOutputStreamOperator<AccessEvent> indoor = patternStream.select(new PatternSelectFunction<AccessEvent, AccessEvent>() {
            @Override
            public AccessEvent select(Map<String, List<AccessEvent>> map) throws Exception {
                return map.get("indoor").get(0);
            }
        });

        OutputTag<AccessEvent> outputTag = new OutputTag<AccessEvent>("timeout"){
            private static final long serialVersionUID=1L;
        };

        SingleOutputStreamOperator<AccessEvent> timeout = patternStream.flatSelect(outputTag, new AccessTimeOut(), new FlatSelect());

        timeout.getSideOutput(outputTag).print();
        timeout.print();

        env.execute("doors");
    }
}
