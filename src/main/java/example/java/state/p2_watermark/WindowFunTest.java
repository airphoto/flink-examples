package example.java.state.p2_watermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/8/6
 **/
public final class WindowFunTest implements WindowFunction<Tuple2<String,Long>,Tuple6<String,Integer,String,String,String,String>,String,TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple6<String, Integer, String, String, String, String>> collector) throws Exception {
        Long max = 0L;
        Long min = Long.MAX_VALUE;
        int size = 0;
        for (Tuple2<String, Long> stringLongTuple2 : iterable) {
            size += 1;
            if (stringLongTuple2.f1 > max) max = stringLongTuple2.f1;
            if (stringLongTuple2.f1 < min) min = stringLongTuple2.f1;
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        collector.collect(new Tuple6<>(s,size,format.format(min),format.format(max),format.format(timeWindow.getStart()),format.format(timeWindow.getEnd())));
    }
}
