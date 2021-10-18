package example.java.functions.windows;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassNameShowWindowFunction
 * @Description
 *
 *      WindowFunction<IN, OUT, KEY, W extends Window>
 *
 * @Author lihuasong
 * @Date2020/4/19 10:22
 * @Version V1.0
 **/
public class ShowWindowFunction implements WindowFunction<Tuple3<String,Long,Long>,String, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, Long, Long>> iterable, Collector<String> collector) throws Exception {
        String key = tuple.toString();

        List<Long> arrayList = new ArrayList<>();

        Iterator<Tuple3<String,Long,Long>> it = iterable.iterator();

        while (it.hasNext()){
            Tuple3<String,Long,Long> next = it.next();
            arrayList.add(next.f1);
        }
        Collections.sort(arrayList);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String result = "触发窗口输出："+key + ","+
                arrayList.size()+","+
                format.format(arrayList.get(0))+","+
                format.format(arrayList.get(arrayList.size()-1))+"," +
                timeWindow.toString()+","+
                format.format(timeWindow.getStart())+","+
                format.format(timeWindow.getEnd());

        collector.collect(result);
    }
}
