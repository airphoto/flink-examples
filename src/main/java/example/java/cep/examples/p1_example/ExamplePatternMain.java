package example.java.cep.examples.p1_example;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName ExamplePatternMain
 * @Author lihuasong
 * @Description
 *
 * 本例中事件模板为当五秒中出现三次名字为login的数据之后，接下来再出现两次名字为login的数据，则触发该事件
 *
 *  数据格式   "1,login,"+System.currentTimeMillis();
 * @Date 2021-10-25 19:36
 * @Version V1.0
 **/

public class ExamplePatternMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","cep_example_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("topic_2_partitions",new SimpleStringSchema(),properties);

        DataStream<String> stream = env.addSource(source);

        SingleOutputStreamOperator<SingleEvent> singleEvent = stream.map(x -> {
            String[] field = x.split(",");
            return new SingleEvent(field[0], field[1], Long.parseLong(field[2]));
        }).assignTimestampsAndWatermarks(new EventTimeAndWatermark(1000L));

        SkipPastLastStrategy skipPastLastStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        Pattern<SingleEvent, SingleEvent> singleEventPattern = Pattern
                .<SingleEvent>begin("start",skipPastLastStrategy)
                .times(3).within(Time.seconds(5))
                .next("middle").subtype(SingleEvent.class).where(
                        new IterativeCondition<SingleEvent>() {
                            @Override
                            public boolean filter(SingleEvent singleEvent, Context<SingleEvent> context) throws Exception {
                                return "login".equals(singleEvent.getEventType());
                            }
                        }
                ).followedBy("end").where(
                        new IterativeCondition<SingleEvent>() {
                            @Override
                            public boolean filter(SingleEvent singleEvent, Context<SingleEvent> context) throws Exception {
                                return "login".equals(singleEvent.getEventType());
                            }
                        }
                );


        PatternStream<SingleEvent> patternStream = CEP.pattern(singleEvent, singleEventPattern);

        patternStream.process(new PatternProcessFunction<SingleEvent, String>() {
            @Override
            public void processMatch(Map<String, List<SingleEvent>> map, Context context, Collector<String> collector) throws Exception {
                String result = JSON.toJSONString(map);
                collector.collect(result);
            }
        }).print();

        env.execute("cep");
    }
}
