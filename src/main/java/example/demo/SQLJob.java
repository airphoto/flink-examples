package example.demo;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/21
 **/
public class SQLJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.enableCheckpointing(1000l);
//
//        FsStateBackend fsStateBackend = new FsStateBackend("file:///D:\\gitlab\\personal\\flink-example\\checkpoint");
//
//        env.setStateBackend(fsStateBackend);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();


//        TableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        List<String> allLines = Files.readAllLines(Paths.get("E:\\idea\\git\\flink\\src\\main\\resources\\q3.sql"));

        String[] lines = allLines.stream().filter((str)->!str.contains("--")).reduce((x1,y1)-> x1.trim()+" "+y1.trim()).get().split(";");

        for (String string : lines){
            System.out.println(string);
            if (string.trim().toLowerCase().startsWith("set")){
                System.out.println("setting : "+string.trim() );
                String[] kv = string.trim().split(" ")[1].split("=");
                tableEnvironment.getConfig().getConfiguration().setString(kv[0],kv[1]);
            }

            if (string.trim().toLowerCase().startsWith("create")){
                System.out.println("create : "+string.trim());
                tableEnvironment.sqlUpdate(string.trim());
            }

            if(string.trim().toLowerCase().startsWith("insert")){
                System.out.println("insert : "+string.trim());
                tableEnvironment.sqlUpdate(string.trim());
            }
        }

        tableEnvironment.execute("table job");
    }

}
