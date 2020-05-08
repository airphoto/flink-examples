package com.lhs.flink.jobs.comprehensive.utils;

import com.lhs.flink.jobs.comprehensive.dao.JdbcUtils;
import com.lhs.flink.jobs.comprehensive.pojo.JobConfig;
import org.apache.flink.table.api.TableEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SchedulerUtils {

    volatile static Set<String> remain = new HashSet<>();

    volatile static List<String> newDescs = new ArrayList<>();

    public static void getJobConfigs() throws SQLException {
        Connection connection = JdbcUtils.getConnection();
        PreparedStatement jobDescs = connection.prepareStatement("select job_desc from flink_job_descs");
        ResultSet resultSet = jobDescs.executeQuery();
        while (resultSet.next()){
            String job_desc = resultSet.getString("job_desc");
            if(!remain.contains(job_desc)){
                remain.add(job_desc);
                newDescs.add(job_desc);
            }
        }
    }

    public static void setFlinkJobs(TableEnvironment tableEnvironment){
        Runnable task = new Runnable() {
            @Override
            public void run() {
                setFlinkInitJobs(tableEnvironment);
            }
        };

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(task,0,10, TimeUnit.SECONDS);
    }


    public static void setFlinkInitJobs(TableEnvironment tableEnvironment){
        try {
            getJobConfigs();
            if (!newDescs.isEmpty()){
                for (String newDesc : newDescs) {
                    System.out.println(newDesc);
                    if(newDesc.trim().toLowerCase().startsWith("set")){
                        String[] kv = newDesc.trim().split(" ")[1].split("=");
                        tableEnvironment.getConfig().getConfiguration().setString(kv[0],kv[1]);
                    }else{
                        tableEnvironment.sqlUpdate(newDesc);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
