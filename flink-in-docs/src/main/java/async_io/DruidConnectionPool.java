package async_io;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @ClassName DruidConnectionPool
 * @Author lihuasong
 * @Description
 * @Date 2022/4/5 16:36
 * @Version V1.0
 **/
public class DruidConnectionPool {
    private transient static DataSource dataSource;
    private transient static Properties properties = new Properties();

    static {
        properties.put("driverClassName","com.mysql.jdbc.Driver");
        properties.put("url","jdbc:mysql://localhost:3306/test");
        properties.put("username","root");
        properties.put("password","qwer1234");
        properties.put("useSSL","true");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private DruidConnectionPool(){}

    public static Connection getConnection() throws Exception{
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    public static void commit(Connection connection){
        if(connection!=null){
            try {
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(connection);
            }
        }
    }

    public static void rollback(Connection connection){
        if(connection!=null){
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(connection);
            }
        }
    }

    public static void close(Connection connection){
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
