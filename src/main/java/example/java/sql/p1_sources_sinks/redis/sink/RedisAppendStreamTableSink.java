package example.java.sql.p1_sources_sinks.redis.sink;

import com.lhs.flink.example.java.state.sink.RedisSinkInstance;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/22
 **/
public class RedisAppendStreamTableSink implements AppendStreamTableSink<Row> {
    private TableSchema schema;

    public RedisAppendStreamTableSink(){
        TableSchema.Builder field = TableSchema.builder()
                .field("k", DataTypes.STRING())
                .field("f", DataTypes.STRING())
                .field("v", DataTypes.STRING());
        this.schema = field.build();
    }
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.print();
    }

    @Override
    public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public DataType getConsumedDataType() {
        return schema.toRowDataType();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        System.out.println("consumeDataStream");
        return dataStream.addSink(new RedisSinkInstance<>());
    }
}
