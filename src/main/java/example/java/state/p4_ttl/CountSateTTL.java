package example.java.state.p4_ttl;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author lihuasong
 * @description
 *
 *  配置状态的过期时间
 *
 * @create 2020/4/13
 **/
public class CountSateTTL extends RichFlatMapFunction<Tuple2<String,Long>, Tuple2<String,Long>> {

    ValueState<Tuple2<String,Long>> sum ;

    @Override
    public void flatMap(Tuple2<String, Long> stringLongTuple2, Collector<Tuple2<String, Long>> collector) throws Exception {

        Tuple2<String,Long> current = sum.value();

        if(current!=null){
            sum.update(new Tuple2<>(current.f0,current.f1+stringLongTuple2.f1));
            collector.collect(new Tuple2<>(current.f0,sum.value().f1));
        }else{
            sum.update(stringLongTuple2);
            collector.collect(stringLongTuple2);
        }


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<String,Long>> descriptor =
                new ValueStateDescriptor<Tuple2<String, Long>>(
                        "text state",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})
                );

        // 配置状态保留的时间长度
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(5))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        // 配置状态的ttl
        descriptor.enableTimeToLive(ttlConfig);

        sum = getRuntimeContext().getState(descriptor);
    }


}
