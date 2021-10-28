package example.java.cep.examples.p1_example;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @ClassName EventTimeAndWatermark
 * @Author lihuasong
 * @Description
 * @Date 2021-10-25 19:41
 * @Version V1.0
 **/

public class EventTimeAndWatermark implements AssignerWithPeriodicWatermarks<SingleEvent> {

    private long currentMaxTimeStamp;
    private long maxOutOfOrder;

    public EventTimeAndWatermark(Long maxOutOfOrder) {
        this.maxOutOfOrder = maxOutOfOrder;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimeStamp-maxOutOfOrder);
    }

    @Override
    public long extractTimestamp(SingleEvent singleEvent, long l) {
        currentMaxTimeStamp = Math.max(singleEvent.getEventTime(),currentMaxTimeStamp);
        System.out.println(JSON.toJSONString(singleEvent));
        return singleEvent.getEventTime();
    }
}
