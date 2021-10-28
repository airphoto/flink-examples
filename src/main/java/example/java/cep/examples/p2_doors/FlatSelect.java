package example.java.cep.examples.p2_doors;

import com.alibaba.fastjson.JSON;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @ClassName FlatSelect
 * @Author lihuasong
 * @Description
 * @Date 2021-10-27 18:59
 * @Version V1.0
 **/

public class FlatSelect implements PatternFlatSelectFunction<AccessEvent,AccessEvent> {
    @Override
    public void flatSelect(Map<String, List<AccessEvent>> map, Collector<AccessEvent> collector) throws Exception {
        System.out.print("正常出入");
        System.out.println(JSON.toJSONString(map));
//        collector.collect(new AccessEvent());
    }
}
