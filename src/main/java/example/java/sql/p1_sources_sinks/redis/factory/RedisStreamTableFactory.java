package example.java.sql.p1_sources_sinks.redis.factory;

import com.lhs.flink.example.java.sql.p1_sources_sinks.redis.sink.RedisAppendStreamTableSink;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/22
 **/
public class RedisStreamTableFactory implements StreamTableSinkFactory<Row> {
    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        System.out.println("StreamTableSink");
        return new RedisAppendStreamTableSink();
    }

    @Override
    public Map<String, String> requiredContext() {
        System.out.println("requiredContext");
        Map<String,String> context = new HashMap<>();
        context.put("connector.type","redis");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        System.out.println("supportedProperties");
        List<String> list = new ArrayList<>();
        list.add("connector.type");
        list.add("connector.debug");
        list.add("schema.#.name");
        list.add("schema.#.data-type");
        list.add("update-mode");
        list.add("connector.property-version");

        return list;
    }
}
