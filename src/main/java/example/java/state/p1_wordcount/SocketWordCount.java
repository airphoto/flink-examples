package example.java.state.p1_wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/7/25
 **/
public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStream<Tuple2<String,Integer>> dataStream = env
                .socketTextStream("local",9999)
                .flatMap((String line, Collector<Tuple2<String,Integer>> out)->
                        Arrays.stream(line.split(" ")).forEach(str->
                            out.collect(new Tuple2<String,Integer>(str,1))
                        )
                ).keyBy(0)
                .sum(1);
        dataStream.print();

        env.execute("word count");

    }


}
