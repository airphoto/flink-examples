package example.java.sql.p1_sources_sinks.print;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PrintSink<IN> extends RichSinkFunction<IN> {
    @Override
    public void invoke(IN value, Context context) throws Exception {
        System.out.println(value);
    }
}
