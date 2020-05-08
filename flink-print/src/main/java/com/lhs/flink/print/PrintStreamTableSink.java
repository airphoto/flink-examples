package com.lhs.flink.print;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class PrintStreamTableSink implements RetractStreamTableSink<Row> {
    private TableSchema schema;

    public PrintStreamTableSink() {
    }

    public PrintStreamTableSink(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return schema.toRowType();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        dataStream.addSink(new PrintSink<>());
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new PrintSink<>());
    }
}
