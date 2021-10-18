package example.java.sql.udf.source;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassNameMySystemTableSourceFactory
 * @Description
 * @Author lihuasong
 * @Date2020/4/25 18:15
 * @Version V1.0
 **/
public class MySystemTableSourceFactory implements StreamTableSourceFactory<Row> {
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
        boolean isDebug = Boolean.valueOf(map.get("connector.debug"));
        return new MySystemAppendTableSource(isDebug);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("update-mode", "append");
        context.put("connector.type", "my-system");

        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("connector.debug");
        list.add("schema.#.data-type");
        list.add("schema.#.name");
        list.add("connector.property-version");
        return list;
    }
}
