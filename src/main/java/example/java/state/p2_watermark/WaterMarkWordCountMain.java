package example.java.state.p2_watermark;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/8/6
 **/
public class WaterMarkWordCountMain {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","flink_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("flink",new SimpleStringSchema(),properties);

        DataStream<String> stream = env.addSource(source);

        DataStream<Tuple2<String,Long>> withTimestampsAndWatermarks = stream.map((line)->{
            String[] field = line.split(",");
            return new Tuple2<String,Long>(field[0],Long.parseLong(field[1]));
        }).returns(Types.TUPLE(Types.STRING,Types.LONG))
         .assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(1000L));

        withTimestampsAndWatermarks
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunTest2())
        .print();



        env.execute("water mark word count");
    }


    /**
     * @author lihuasong
     * @description 描述
     * @create 2019/8/6
     **/
    public static final class WindowFunTest2 implements WindowFunction<Tuple2<String,Long>,Tuple6<String,Integer,String,String,String,String>,Tuple,TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple6<String, Integer, String, String, String, String>> collector) throws Exception {
            Long max = 0L;
            Long min = Long.MAX_VALUE;
            int size = 0;
            for (Tuple2<String, Long> stringLongTuple2 : iterable) {
                size += 1;
                if (stringLongTuple2.f1 > max) max = stringLongTuple2.f1;
                if (stringLongTuple2.f1 < min) min = stringLongTuple2.f1;
            }
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            collector.collect(new Tuple6<>(tuple.getField(0),size,format.format(min),format.format(max),format.format(timeWindow.getStart()),format.format(timeWindow.getEnd())));
        }
    }

}
