package example.java.state.p8_window.utils;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @ClassName WindowCountAggregation
 * @Author lihuasong
 * @Description
 * @Date 2021-10-19 20:50
 * @Version V1.0
 **/

public class WindowCountAggregation implements AggregateFunction<WindowCountEvent,Long, Tuple2<String,Long>>{

    private String word;
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(WindowCountEvent windowCountEvent, Long aLong) {
        this.word = windowCountEvent.getWord();
        return windowCountEvent.getValue()+aLong;
    }

    @Override
    public Tuple2<String, Long> getResult(Long aLong) {
        return new Tuple2<>(word,aLong);
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}
