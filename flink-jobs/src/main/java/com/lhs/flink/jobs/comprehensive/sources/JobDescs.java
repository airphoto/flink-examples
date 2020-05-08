package com.lhs.flink.jobs.comprehensive.sources;

import com.lhs.flink.jobs.comprehensive.dao.JdbcUtils;
import com.lhs.flink.jobs.comprehensive.pojo.JobConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class JobDescs extends RichSourceFunction<List<JobConfig>> {

    private boolean running = true;

    @Override
    public void run(SourceContext<List<JobConfig>> sourceContext) throws Exception {
        while (running){
            List<JobConfig> configs = new ArrayList<>();
            Connection connection = JdbcUtils.getConnection();
            PreparedStatement jobDescs = connection.prepareStatement("select job_desc from flink_job_descs");
            ResultSet resultSet = jobDescs.executeQuery();
            while (resultSet.next()){
                String job_desc = resultSet.getString("job_desc");
                JobConfig jobConfig = new JobConfig();
                jobConfig.setJobdesc(job_desc);
                configs.add(jobConfig);
            }
            sourceContext.collect(configs);
            JdbcUtils.close(resultSet,jobDescs,connection);
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
