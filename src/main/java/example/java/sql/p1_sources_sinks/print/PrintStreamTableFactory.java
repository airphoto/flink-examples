package example.java.sql.p1_sources_sinks.print;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrintStreamTableFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {
    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> map) {

        TableSchema.Builder builder = TableSchema.builder();
        long schemaLength = map.keySet().stream().filter(x -> x.startsWith("schema")).count() / 2;

        for (int i = 0; i < schemaLength; i++) {
            String name = map.get("schema."+i+".name");
            String type = map.get("schema."+i+".data-type");
            System.out.println(name+"-"+type);
            DataType dataType = TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(type));
            builder.field(name, dataType);
        }

        return new PrintStreamTableSink(builder.build());
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String,String> map = new HashMap<>();
        map.put("connector.type","print");
        return map;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList();
        properties.add("schema.#.data-type");
        properties.add("schema.#.name");
        return properties;
    }
}
