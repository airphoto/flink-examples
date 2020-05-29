package com.lhs.flink.jobs.comprehensive;

import com.lhs.flink.jobs.comprehensive.utils.SchedulerUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class JobMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.enableCheckpointing(2000);
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnvironment = StreamTableEnvironment.create(environment,settings);

        Configuration configuration = tableEnvironment.getConfig().getConfiguration();

        tableEnvironment.getConfig().setIdleStateRetentionTime(Time.seconds(10),Time.seconds(320));;

        tableEnvironment.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled","true");
        tableEnvironment.getConfig().getConfiguration().setString("table.exec.mini-batch.allow-latency","60000 ms");
        tableEnvironment.getConfig().getConfiguration().setString("table.exec.mini-batch.size","100000000");

        SchedulerUtils.setFlinkInitJobs(tableEnvironment);
        tableEnvironment.execute("running jobs");
    }

}
