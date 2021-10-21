package example.java.state.p6_broadcast;

import example.java.state.pojo.JobConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/15
 **/
public class MyKeydBroadcastProcessFunc extends KeyedBroadcastProcessFunction<String,Tuple2<String,Long>,Map<String,JobConfig>,Tuple2<String,Long>> {

    private JobConfig jobConfig;

    @Override
    public void processElement(Tuple2<String, Long> stringLongTuple2, ReadOnlyContext readOnlyContext, Collector<Tuple2<String,Long>> collector) throws Exception {
        String[] split = this.jobConfig.getValue().split(",");
        Set<String> keys = new HashSet<>();
        for (String s : split) {
            keys.add(s);
        }

        Tuple2<String,Long> value = new Tuple2<>();
        if (keys.contains(stringLongTuple2.f0)){
            value.setFields(stringLongTuple2.f0,stringLongTuple2.f1*10);
        }else{
            value.setFields(stringLongTuple2.f0,stringLongTuple2.f1);
        }

        collector.collect(value);
    }

    @Override
    public void processBroadcastElement(Map<String, JobConfig> stringJobConfigMap, Context context, Collector<Tuple2<String,Long>> collector) throws Exception {
        this.jobConfig = stringJobConfigMap.get("adids");
    }
}
