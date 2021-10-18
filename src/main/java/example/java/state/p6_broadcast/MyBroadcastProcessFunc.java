package example.java.state.p6_broadcast;

import com.lhs.flink.example.java.state.pojo.JobConfig;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/15
 **/
public class MyBroadcastProcessFunc extends BroadcastProcessFunction<String,Map<String,JobConfig>,String> {

    private  JobConfig jobConfig;

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        String jobConfigDesc = jobConfig.toString();
        String value = s+"->"+jobConfigDesc;

        collector.collect(value);
    }

    @Override
    public void processBroadcastElement(Map<String, JobConfig> stringJobConfigMap, Context context, Collector<String> collector) throws Exception {
        this.jobConfig = stringJobConfigMap.get("adids");
    }
}
