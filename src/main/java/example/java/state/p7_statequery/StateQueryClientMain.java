package example.java.state.p7_statequery;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

/**
 * Created by abel on 2020/4/18.
 *
 * 需要将 flink-queryable-state-runtime_2.11-1.9.2.jar 放到集群的lib文件夹下
 *
 * 设置
 * descriptor.setQueryable("query-state");
 *
 * 配置 conf/flink-conf.yaml 的文件
 *      queryable-state.enable: true
 *      默认端口  9069（代理端口）
 */
public class StateQueryClientMain {
    public static void main(String[] args) throws Exception {

        QueryableStateClient client = new QueryableStateClient("127.0.0.1",9069);

        ValueStateDescriptor<Long> stateDescriptor =
                new ValueStateDescriptor<Long>(
                        "state-query",
                        TypeInformation.of(new TypeHint<Long>() {})
                );

        final JobID jobid = JobID.fromHexString("fc636906c35b1b7932dbaa02952347cb");

        while (true){
            CompletableFuture<ValueState<Long>> completableFuture =
                    client.getKvState(
                            jobid,
                            "window_count_state",
                            "word",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            stateDescriptor
                    );

            System.out.println(completableFuture.get().value());

            Thread.sleep(1000);
        }

    }
}
