package com.lhs.flink.jobs.comprehensive.dao;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JdbcUtils {
    //url
    private static String url = null;
    //user
    private static String user = null;
    //password
    private static String password = null;
    //驱动程序类
    private static String driverClass = null;
    /**
     * 只注册一次，静态代码块
     */
    static{

        //注册驱动程序
        try {

            /**
             * 读取jdbc.properties文件
             */
            //1)创建Properties对象
            Properties prop = new Properties();
            //构造输入流
            /**
             * 相对路径：  .  这个点代表当前目录。当前目录本质上是java命令运行的目录
             * java项目：  在ecplise中，当前目录指向项目的根目录。
             * web项目： 当前目录指向%tomcat%/bin目录
             *   1)结论： 在web项目不能使用相对路径
             *
             *   web项目中加载配置文件： ServletContext.getRealPath()  /  getResourceAsStream() 这种方式对于jdbcUtil这个工具而言，放到java项目中找不到ServletContext对象，不通用的！
             *   2)不能使用ServletContext读取文件
             *
             *   3）使用类路径方式读取配置文件
             *
             */
            //1)获取类的对象
            Class clazz = JdbcUtils.class;
            //2) 使用类路径的读取方法去读取文件
            /**
             *   这个斜杠：代表项目的类路径的根目录。  类路径： 查询类的目录/路径
             *   java项目下： 类路径的根目录，指向项目的bin目录
             *   web项目下：类路径的根目录，指向项目的WEB-INF/classes目录
             *
             *   只有把配置文件放在src目录的根目录下，那么这些文件就会自动拷贝到项目的类路径根目录下。
             */
            InputStream in = clazz.getResourceAsStream("/jdbc.properties");
            // File f = new File(clazz.getResource("/").getFile());
            // System.out.println(f.getAbsolutePath());
            //构造输入流
            // InputStream in = new FileInputStream("./src/jdbc.properties");
            //2)加载文件
            prop.load(in);
            //3)读取文件内容
            url = prop.getProperty("url");
            user = prop.getProperty("user");
            password = prop.getProperty("password");
            driverClass = prop.getProperty("driverClass");

            // 测试是否从properties中读到
            System.out.println(url);
            System.out.println(user);
            System.out.println(password);
            System.out.println(driverClass);

            Class.forName(driverClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * 获取连接方法
     */
    public static Connection getConnection(){
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(Statement stmt, Connection conn){
        if(stmt!=null){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn){
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        close(stmt,conn);
    }
}
