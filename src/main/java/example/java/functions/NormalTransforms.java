package example.java.functions;

import com.lhs.flink.example.java.functions.pojo.WordCase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 基本的转换函数
 */
public class NormalTransforms {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id","functions");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("flink",new SimpleStringSchema(),properties);

        DataStreamSource<String> streamSource = environment.addSource(source);


//        // MapFunction<IN,OUT>  DataStream -> DataStream
//        streamSource.map(new MapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                return new Tuple2<>(s,1);
//            }
//        }).print();
//

//        // FlatMapFunction<IN,OUT>  DataStream -> DataStream
//        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                for (String i : s.split(" ")){
//                    collector.collect(new Tuple2<>(i,1));
//                }
//            }
//        }).print();

//        // FilterFunction<IN>  DataStream -> DataStream
//        streamSource.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return !s.contains(" ");
//            }
//        }).print();

//        // KeyBy<POJO>  DataStream -> KeyedStream
//        KeyedStream<WordCase, Tuple> keyedStream = streamSource.map(new MapFunction<String, WordCase>() {
//            @Override
//            public WordCase map(String s) throws Exception {
//
//                return new WordCase(s, "field", 1);
//            }
//        }).keyBy("key");
//
        // 当keyBy中的参数值为数字的时候，数据类型不能是pojo类型，只能是tuple类型
        // keyBy<tuple>  DataStream -> KeyedStream
        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream = streamSource.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String s) throws Exception {
                return new Tuple3<>(s, "field", 1);
            }
        }).keyBy(0);

        // Reduce<IN> KeyedStream -> DataStream
//        SingleOutputStreamOperator<Tuple3<String, String, Integer>> outputStreamOperator = keyedStream.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
//            @Override
//            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> stringStringIntegerTuple3, Tuple3<String, String, Integer> t1) throws Exception {
//                return new Tuple3<>(t1.f0, t1.f1, stringStringIntegerTuple3.f2 + t1.f2);
//            }
//        });
//        outputStreamOperator.print();


        // Fold  KeyedStream -> DataStream (deprecate)

        // sum,min,max,maxBy,minBy  KeyedStream -> DataStream
//        keyedStream.sum(2).print();
//        keyedStream.min(2).print();
//        keyedStream.max(1).print();
//        keyedStream.minBy(1).print();
//        keyedStream.maxBy(1).print();

        environment.execute("functions");
    }

}
