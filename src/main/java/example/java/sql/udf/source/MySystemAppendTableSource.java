package example.java.sql.udf.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * @ClassNameMySystemAppendTableSource
 * @Description
 * @Author lihuasong
 * @Date2020/4/25 18:16
 * @Version V1.0
 **/
public class MySystemAppendTableSource implements StreamTableSource<Row> {
    public MySystemAppendTableSource(boolean isDebug) {
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {

        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        TableSchema schema =  TableSchema.builder().field("key", DataTypes.STRING()).build();
        return schema;
    }
}
