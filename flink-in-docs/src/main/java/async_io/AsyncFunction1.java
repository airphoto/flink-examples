package async_io;

import async_io.pojo.CategoryInfo;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;

import java.sql.*;
import java.util.Collections;
import java.util.List;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * @ClassName AsyncFunction1
 * @Author lihuasong
 * @Description
 *
 *      使用java-vertx中提供的异步client实现异步io
 *
 * @Date 2021-09-30 15:20
 * @Version V1.0
 **/

public class AsyncFunction1 extends RichAsyncFunction<CategoryInfo,CategoryInfo> {

    private transient SQLClient sqlClient;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject mysqlClientConfig = new JsonObject();
        mysqlClientConfig.put("driver_class","com.mysql.jdbc.Driver");
        mysqlClientConfig.put("url","jdbc:mysql://10.122.238.97:13306/datacenter");
        mysqlClientConfig.put("user","xl_test");
        mysqlClientConfig.put("password","xianlai@test2018");
        mysqlClientConfig.put("max_pool_size",10);

        VertxOptions options = new VertxOptions();
        options.setEventLoopPoolSize(10);
        options.setWorkerPoolSize(20);
        Vertx vertx = Vertx.vertx(options);

        // 根据上边的配置参数获取异步请求客户端
        sqlClient = JDBCClient.create(vertx,mysqlClientConfig);

    }

    // 使用异步客户端发送异步请求
    @Override
    public void asyncInvoke(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) {
        System.out.println(input);
        sqlClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                if(sqlConnectionAsyncResult.failed()){
                    return;
                }
                SQLConnection sqlConnection = sqlConnectionAsyncResult.result();
                sqlConnection.query("select id,name from t_category where id=" + input.getId(), new Handler<AsyncResult<io.vertx.ext.sql.ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<io.vertx.ext.sql.ResultSet> resultSetAsyncResult) {
                        if(resultSetAsyncResult.succeeded()){
                            List<JsonObject> rows = resultSetAsyncResult.result().getRows();
                            for (JsonObject row : rows) {
                                CategoryInfo categoryInfo = new CategoryInfo(row.getValue("id").toString(),row.getString("name"));
                                resultFuture.complete(Collections.singletonList(categoryInfo));
                            }
                        }
                    }
                });
            }
        });
    }
}
