package example.java.state.sink;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/14
 **/
public class WriteSinkInstance<IN> extends RichSinkFunction<IN> {

    PrintSinkOutputWriter<IN> writer;

    public WriteSinkInstance(){
        this.writer = new PrintSinkOutputWriter<>(false);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open");
        StreamingRuntimeContext context = (StreamingRuntimeContext) this.getRuntimeContext();
        this.writer.open(context.getIndexOfThisSubtask(),context.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        System.out.println("invoke");
        this.writer.write(value);
    }
}
