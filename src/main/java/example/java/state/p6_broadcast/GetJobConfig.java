package example.java.state.p6_broadcast;

import com.lhs.flink.example.java.state.pojo.JobConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/15
 **/
public class GetJobConfig extends RichSourceFunction<Map<String,JobConfig>> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Map<String, JobConfig>> sourceContext) throws Exception {
        while (running){
            Jedis jedis = new Jedis("10.51.237.173",16379);
            jedis.auth("XLhy!321YH");
            jedis.select(2);
            String adids = jedis.get("alg:mt:adids");
            if(adids != null && !adids.isEmpty()){
                JobConfig jobConfig = new JobConfig();
                jobConfig.setKey("alg:mt:adids");
                jobConfig.setValue(adids);
                Map<String,JobConfig> map = new HashMap<>();
                map.put("adids",jobConfig);
                sourceContext.collect(map);
            }
            jedis.close();
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
