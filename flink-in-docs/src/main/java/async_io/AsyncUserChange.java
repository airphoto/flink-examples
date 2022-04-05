package async_io;

import async_io.pojo.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import scala.annotation.meta.param;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @ClassName AsyncUserChange
 * @Author lihuasong
 * @Description
 * @Date 2022/4/5 10:07
 * @Version V1.0
 **/
public class AsyncUserChange extends RichAsyncFunction<UserProfile, UserChange> {
    private transient SQLClient sqlClient;
    private Map<String,String> fieldType = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject mysqlClientConfig = new JsonObject();
        mysqlClientConfig.put("driver_class","com.mysql.jdbc.Driver");
        mysqlClientConfig.put("url","jdbc:mysql://localhost:3306/test");
        mysqlClientConfig.put("user","root");
        mysqlClientConfig.put("password","qwer1234");
        mysqlClientConfig.put("max_pool_size",11);

        VertxOptions options = new VertxOptions();
        options.setEventLoopPoolSize(10);
        options.setWorkerPoolSize(20);
        Vertx vertx = Vertx.vertx(options);

        // 根据上边的配置参数获取异步请求客户端
        sqlClient = JDBCClient.create(vertx,mysqlClientConfig);

        fieldType.put("user_id","string");
        fieldType.put("nick_name","string");
        fieldType.put("sex","int");
        fieldType.put("gold","int");
    }

    @Override
    public void asyncInvoke(UserProfile userProfile, ResultFuture<UserChange> resultFuture) throws Exception {
        System.out.println(userProfile);

        List<UserChange> result = new ArrayList<>();
        User user = userProfile.getUser();
        // 如果是只插入的话，就不需要查mysql了
        Stream<Param> paramOnlySet = userProfile.getParam().stream().filter(x -> "attr_set".equals(x.getChange_type()));
        for (Param param : paramOnlySet.collect(Collectors.toList())) {
            result.add(new UserChange(user.getApp_id(),user.getXl_id(),param.getField_name(),param.getField_value(),fieldType.get(param.getField_name())));
        }

        // 需要从mysql中查数据
        List<Param> paramShouldConnectMysql = userProfile.getParam().stream().filter(x -> !"attr_set".equals(x.getChange_type())).collect(Collectors.toList());
        if(paramShouldConnectMysql.isEmpty()){
            resultFuture.complete(result);
            return;
        }

        String selectFields = String.join(",",paramShouldConnectMysql.stream().map(x->x.getField_name()).collect(Collectors.toList()));
        Map<String,String> mapValue = new HashMap<>();
        Map<String,String> mapType = new HashMap<>();
        for (Param param : paramShouldConnectMysql) {
            mapValue.put(param.getField_name(),param.getField_value());
            mapType.put(param.getField_name(),param.getChange_type());
        }

        sqlClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                if(sqlConnectionAsyncResult.failed()){
                    return;
                }
                SQLConnection sqlConnection = sqlConnectionAsyncResult.result();
                String sql = "select "+selectFields+" from user_"+userProfile.getUser().getApp_id() + " where xl_id='" + userProfile.getUser().getXl_id()+"'";
                System.out.println(sql);
                sqlConnection.query(sql, new Handler<AsyncResult<io.vertx.ext.sql.ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<io.vertx.ext.sql.ResultSet> resultSetAsyncResult) {
                        if(resultSetAsyncResult.succeeded()){

                            List<JsonObject> rows = resultSetAsyncResult.result().getRows();
                            if(rows.isEmpty()){
                                for (Map.Entry<String, String> entry : mapValue.entrySet()) {
                                    if(mapType.get(entry.getKey()).equals("attr_add")){
                                        result.add(new UserChange(user.getApp_id(),user.getXl_id(),entry.getKey(),entry.getValue(),fieldType.get(entry.getKey())));
                                    }else if (mapType.get(entry.getKey()).equals("attr_append")){
                                        result.add(new UserChange(user.getApp_id(),user.getXl_id(),entry.getKey(),"[\""+mapValue.get(entry.getKey())+"\"]",fieldType.get(entry.getKey())));
                                    }else if(mapType.get(entry.getKey()).equals("set_once")){
                                        result.add(new UserChange(user.getApp_id(),user.getXl_id(),entry.getKey(),mapValue.get(entry.getKey()),fieldType.get(entry.getKey())));
                                    }
                                }
                            } else {
                                for (JsonObject row : rows) {
                                    System.out.println("mysql 原始数据" + row.toString());

                                    for (Map.Entry<String, Object> entry : row) {
                                        if (mapType.get(entry.getKey()).equals("attr_add")) {
                                            Integer value = Integer.parseInt(mapValue.getOrDefault(entry.getKey(), "0")) + Integer.parseInt(entry.getValue() == null ? "0" : entry.getValue().toString());
                                            result.add(new UserChange(user.getApp_id(), user.getXl_id(), entry.getKey(), value.toString(), fieldType.get(entry.getKey())));
                                        } else if (mapType.get(entry.getKey()).equals("attr_append")) {
                                            if (entry.getValue() == null) {
                                                result.add(new UserChange(user.getApp_id(), user.getXl_id(), entry.getKey(), "[\"" + mapValue.get(entry.getKey()) + "\"]", fieldType.get(entry.getKey())));
                                            } else {
                                                JSONArray objects = JSON.parseArray(entry.getValue().toString());
                                                if (objects.size() <= 500 && !objects.contains(mapValue.get(entry.getKey()))) {
                                                    objects.add(mapValue.get(entry.getKey()));
                                                    result.add(new UserChange(user.getApp_id(), user.getXl_id(), entry.getKey(), objects.toString(), fieldType.get(entry.getKey())));
                                                }
                                            }
                                        } else if (mapType.get(entry.getKey()).equals("set_once")) {
                                            if (entry.getValue() == null) {
                                                result.add(new UserChange(user.getApp_id(), user.getXl_id(), entry.getKey(), mapValue.get(entry.getKey()), fieldType.get(entry.getKey())));
                                            }
                                        }
                                    }
                                }
                            }
                            resultFuture.complete(result);
                        }else{
                            System.out.println("not ok");
                        }
                    }
                });
            }
        });
    }

    @Override
    public void timeout(UserProfile input, ResultFuture<UserChange> resultFuture) throws Exception {
        System.out.println("timeout :"+input);
    }
}
