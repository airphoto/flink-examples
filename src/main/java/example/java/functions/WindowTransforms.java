package example.java.functions;

import example.java.functions.watermark.MyAssingTimestampAndWatermarks;
import example.java.functions.windows.ShowWindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @ClassNameWindowTransforms
 * @Description
 *
 *      Window相关的转换函数的使用
 *
 * @Author lihuasong
 * @Date2020/4/18 21:29
 * @Version V1.0
 **/
public class WindowTransforms {

    public static void main(String[] args) throws Exception {

        Configuration configuraton = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuraton);

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id","window");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("flink",new SimpleStringSchema(),properties);

        DataStreamSource<String> streamSource = environment.addSource(source);

        SingleOutputStreamOperator<Tuple3<String,Long, Long>> dataAndWatermarks = streamSource
                .filter((s)-> s.split(",").length == 2)
                .returns(TypeInformation.of(new TypeHint<String>() {}))
                .map(new MapFunction<String, Tuple3<String,Long, Long>>() {
                    @Override
                    public Tuple3<String,Long, Long> map(String s) throws Exception {
                        String[] field = s.split(",");
                        return Tuple3.of(field[0],Long.parseLong(field[1]), 1L);
                    }
                }).returns(TypeInformation.of(new TypeHint<Tuple3<String,Long,Long>>() {}))
                .assignTimestampsAndWatermarks(new MyAssingTimestampAndWatermarks(5000L));


        KeyedStream<Tuple3<String, Long, Long>, Tuple> keyedStream = dataAndWatermarks.keyBy(0);

        /**
         *  滚动窗口大小为2s
         *  window的效果和timeWindow一样
         * */
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .apply(new ShowWindowFunction())
                .print();


        environment.execute("window stream");

    }

}
