package example.java.state.p8_window;

import example.java.state.p8_window.utils.WindowCountAggregation;
import example.java.state.p8_window.utils.WindowCountAssignerWithPeriodicWaterMarks;
import example.java.state.p8_window.utils.WindowCountEvent;
import example.java.state.p8_window.utils.WindowCountProcessFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @ClassName WindowMain
 * @Author lihuasong
 * @Description
 *
 * 测试 滚动，滑动，会话 窗口
 * @Date 2021-10-19 19:28
 * @Version V1.0
 **/

public class WindowMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","window_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("topic_2_partitions",new SimpleStringSchema(),properties);

        DataStream<String> stream = env.addSource(source);

        SingleOutputStreamOperator<WindowCountEvent> windowCountEvents = stream
                .filter(x-> x.split(",").length==3)
                .map(x -> {
                    String[] fields = x.split(",");
                    return new WindowCountEvent(Long.parseLong(fields[0]), fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(new WindowCountAssignerWithPeriodicWaterMarks(5000L));

//        // 滚动窗口
//        SingleOutputStreamOperator<String> tumblingAgg = windowCountEvents
//                .keyBy(WindowCountEvent::getWord)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .aggregate(new WindowCountAggregation(), new WindowCountProcessFunction());
//        tumblingAgg.print();

        // 滑动窗口，窗口大小10秒，滑动大小5秒
        SingleOutputStreamOperator<Tuple2<String, Long>> slideAgg = windowCountEvents
                .keyBy(WindowCountEvent::getWord)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .aggregate(new WindowCountAggregation());
        slideAgg.print();

        env.execute("window_count");

    }

}
