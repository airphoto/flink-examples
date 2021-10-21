package example.java.state.p8_window.utils;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @ClassName WindowCountAssignerWithPeriodicWaterMarks
 * @Author lihuasong
 * @Description
 * @Date 2021-10-19 19:35
 * @Version V1.0
 **/

public class WindowCountAssignerWithPeriodicWaterMarks implements AssignerWithPeriodicWatermarks<WindowCountEvent> {

    private long maxOutOfOrder;
    private long currentMaxTimestamp;
    private Watermark watermark;

    public WindowCountAssignerWithPeriodicWaterMarks(Long maxOutOfOrder) {
        this.maxOutOfOrder = maxOutOfOrder;
    }

    // 根据 出现的最大的时间戳 - 最大容忍延迟  生成水位线
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        watermark = new Watermark(currentMaxTimestamp - maxOutOfOrder);
        return watermark;
    }

    // 从消息中提取事件时间
    @Override
    public long extractTimestamp(WindowCountEvent windowCountEvent, long l) {
        Long eventTime = windowCountEvent.getTimestamp();
        currentMaxTimestamp = Math.max(eventTime,currentMaxTimestamp);
//        System.out.println("current:"+currentMaxTimestamp+" event:"+windowCountEvent.getTimestamp()+" watermark:"+watermark.toString());
        return eventTime;
    }
}
