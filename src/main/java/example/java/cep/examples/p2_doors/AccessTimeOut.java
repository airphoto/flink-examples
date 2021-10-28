package example.java.cep.examples.p2_doors;

import com.alibaba.fastjson.JSON;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @ClassName AccessTimeOut
 * @Author lihuasong
 * @Description
 * @Date 2021-10-27 18:58
 * @Version V1.0
 **/

public class AccessTimeOut implements PatternFlatTimeoutFunction<AccessEvent,AccessEvent> {
    @Override
    public void timeout(Map<String, List<AccessEvent>> map, long l, Collector<AccessEvent> collector) throws Exception {

        if (map.get("outdoor")!=null){
            for (AccessEvent accessEvent : map.get("outdoor")) {
                collector.collect(accessEvent);
            }
        }

        // 因为indoor超时了，还没有收到indoor，所以这里是拿不到indoor的
        System.out.print("异常出入:");
        System.out.println(JSON.toJSONString(map));
    }
}
