package example.java.state.p3_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author lihuasong
 * @description
 *
 *      状态函数
 *
 * @create 2019/8/7
 **/
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

    // ValueState的句柄，第一个值是数量，第一个值是统计值
    private transient ValueState<Tuple2<Long,Long>> sum;
    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {
        // 状态值
        Tuple2<Long,Long> currentSum = sum.value();

        // 更新统计数量
        currentSum.f0 += 1;

        //
        currentSum.f1 += longLongTuple2.f1;

        // 更新状态值
        sum.update(currentSum);

        if(currentSum.f0 >= 2){
            collector.collect(new Tuple2<>(longLongTuple2.f0,currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long,Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average",// 描述信息
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),// type的信息
                        Tuple2.of(0L,0L) // 默认值
                );
        sum = getRuntimeContext().getState(descriptor);
    }
}
