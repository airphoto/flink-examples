package example.java.state.p2_watermark;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author lihuasong
 * @description
 *          描述
 *          数据格式：word,timestamp
 * @create 2019/8/6
 **/
public class WaterMarkWordCountMain {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // 使用Event时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","p2_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("topic_2_partitions",new SimpleStringSchema(),properties);
        DataStream<String> stream = env.addSource(source);


        DataStream<Tuple2<String,Long>> withTimestampsAndWatermarks = stream
                .map((line)->{
                    String[] field = line.split(",");
                    return new Tuple2<>(field[0],Long.parseLong(field[1]));
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(1000L));


        // KeyedStream<T, KEY>  key的类型竟然在后边，还是Tuple类型的
        KeyedStream<Tuple2<String, Long>, Tuple> tuple2TupleKeyedStream = withTimestampsAndWatermarks
                .keyBy(0);

        tuple2TupleKeyedStream

//                .window(TumblingEventTimeWindows.of(Time.seconds(3)))   表示使用eventTime
                .timeWindow(Time.seconds(3))                            // 在 env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) 设置了时间类型之后，默认timeWindow使用eventTime
                .apply(new WindowFunTest2())
                .print();



        env.execute("water mark word count");
    }


    /**
     * @author lihuasong
     * @description
     *
     * public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable {
     *     void apply(KEY var1, W var2, Iterable<IN> var3, Collector<OUT> var4) throws Exception;
     * }
     *
     * 1：缓存窗口内所有的数据 即 Iterable<IN> var3
     * 2：Window 包含当前窗口的信息，包含窗口开始时间，窗口结束时间等
     * 3：
     *
     * @create 2019/8/6
     **/
    public static final class WindowFunTest2 implements WindowFunction<Tuple2<String,Long>, String,Tuple,TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
            Long max = 0L;
            Long min = Long.MAX_VALUE;
            int size = 0;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            for (Tuple2<String, Long> stringLongTuple2 : iterable) {
                System.out.println(stringLongTuple2.f0+"->"+stringLongTuple2.f1);
                size += 1;
                if (stringLongTuple2.f1 > max) max = stringLongTuple2.f1;
                if (stringLongTuple2.f1 < min) min = stringLongTuple2.f1;
            }
            collector.collect("word:["+tuple.toString()+"] count:["+size+"] max:["+format.format(min)+"] min:["+format.format(max)+"] windowStart["+format.format(timeWindow.getStart())+"] windowEnd["+format.format(timeWindow.getEnd())+"]");
        }
    }

}
