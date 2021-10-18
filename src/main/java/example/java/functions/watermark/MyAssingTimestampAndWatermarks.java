package example.java.functions.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.SQLOutput;
import java.text.SimpleDateFormat;

/**
 * @ClassNameMyAssingTimestampAndWatermarks
 * @Description
 *
 * 这是事件时间的分配方式
 * 为每条数据分配  时间戳 和  watermark
 *
 * @Author lihuasong
 * @Date2020/4/19 8:55
 * @Version V1.0
 **/
public class MyAssingTimestampAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<String,Long,Long>> {

    /**
     * 最大延迟时间
     */
    private long maxOutOfOrder;
    private long currentMaxTimestamp;
    private Watermark watermark;
    private SimpleDateFormat format;

    public MyAssingTimestampAndWatermarks() {
    }

    public MyAssingTimestampAndWatermarks(Long maxOutOfOrder){
        this.maxOutOfOrder = maxOutOfOrder;
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        watermark = new Watermark(currentMaxTimestamp-maxOutOfOrder);
        return watermark;
    }

    /**
     *  提取日志中的时间戳，并和currentMaxTimestamp对比，取出最大值
     * @param kv
     * @param l
     * @return
     */
    @Override
    public long extractTimestamp(Tuple3<String,Long,Long> kv, long l) {
        Long eventTime = kv.f1;
        currentMaxTimestamp = Math.max(eventTime,currentMaxTimestamp);

        long id = Thread.currentThread().getId();
        System.out.println(
                "currentTheadId:"+id+",key"+
                        kv.f0+",eventTime:["+
                        kv.f1 +"|"+format.format(new Date(kv.f1))+"],currentMaxTimestamp:["+
                        currentMaxTimestamp+"|"+format.format(new Date(currentMaxTimestamp))+"],watermark"+
                        getCurrentWatermark().getTimestamp()+"|"+format.format(new Date(getCurrentWatermark().getTimestamp()))+"]"

        );

        return currentMaxTimestamp;
    }
}
