package example.java.state.p2_watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @ClassNameMyAssignerWithPunctuatedWatermarks
 * @Description
 *
 *      带断点的水印
 *
 * 每一个事件都会创建新的水印
 *
 * 首先调用 extractTimestamp() 为每个元素分配一个时间戳
 * 然后调用checkAndGetNextWatermark() 方法
 *
 * checkAndGetNextWatermark 方法中传入的是 extractTimestamp 中分配的timestamp
 * 并可以决定是否要生成watermark
 *
 * 没放checkAndGetNextWatermark 方法返回一个非空的watermark
 * 并且该watermark大于最新的前一个watermark时，就会发出新的watermark
 *
 * 需要注意的是：
 *     该方法可以在每个事件上生成一个watermark，由于每个watermark都会导致下游的一计算，所以过多的watermark会降低性能
 *
 * @Author lihuasong
 * @Date2020/4/19 9:24
 * @Version V1.0
 **/
public class MyAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Object o, long l) {
        return null;
    }

    @Override
    public long extractTimestamp(Object o, long l) {
        return 0;
    }
}
