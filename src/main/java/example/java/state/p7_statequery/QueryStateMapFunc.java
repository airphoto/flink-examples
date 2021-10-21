package example.java.state.p7_statequery;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Created by abel on 2020/4/18.
 */
public class QueryStateMapFunc extends RichMapFunction<Tuple2<String,Long>,Tuple2<String,Long>> {

    private transient ValueState<Long> sum;

    @Override
    public Tuple2<String, Long> map(Tuple2<String, Long> stringLongTuple2) throws Exception {
        Long current = sum.value();
        sum.update(current+stringLongTuple2.f1);
        return new Tuple2<>(stringLongTuple2.f0,sum.value());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "query-state-map-func",
                TypeInformation.of(new TypeHint<Long>() {}));

        descriptor.setQueryable("query_state_name");
        sum = getRuntimeContext().getState(descriptor);
    }
}
