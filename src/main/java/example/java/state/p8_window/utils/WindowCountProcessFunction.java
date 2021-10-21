package example.java.state.p8_window.utils;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName WindowCountProcessFunction
 * @Author lihuasong
 * @Description
 * @Date 2021-10-21 18:58
 * @Version V1.0
 **/

public class WindowCountProcessFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

    private transient ValueState<Long> sum;

    static final SimpleDateFormat formatMS = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    static final SimpleDateFormat formatMm = new SimpleDateFormat("ss");

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {

        long startWindow = context.window().getStart();

        Date date = new Date(startWindow);

        if("00".equals(formatMm.format(date))){
            sum.update(0L);
        }

        if(sum.value()==null){
            sum.update(0L);
        }

        for (Tuple2<String, Long> tuple2 : iterable) {
            System.out.println(tuple2.f1+"<-"+tuple2.f0);
            sum.update(sum.value()+tuple2.f1);
        }


        String formatTime = WindowCountProcessFunction.formatMS.format(date);


        String result = "format_time:["+formatTime+"] key:["+ s +"] sum:["+sum.value()+"]";
        collector.collect(result);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "text state",
                        TypeInformation.of(new TypeHint<Long>() {})
                );

        // 配置状态保留的时间长度
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(20))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        // 配置状态的ttl
//        descriptor.enableTimeToLive(ttlConfig);
        descriptor.setQueryable("window_count_state");
        sum = getRuntimeContext().getState(descriptor);
    }
}
