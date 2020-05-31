import com.lhs.flink.dao.LogConfigMapper;
import com.lhs.flink.dao.MybatisSessionFactory;
import com.lhs.flink.pojo.LogConfig;
import org.apache.ibatis.session.SqlSession;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/27
 **/
public class MyBatisTest {
    public static void main(String[] args) throws InterruptedException {
//        SqlSession sqlSession= null;
//        try {
//            sqlSession = MybatisSessionFactory.getSession();
//            LogConfigMapper mapper = sqlSession.getMapper(LogConfigMapper.class);
//            List<LogConfig> logConfigs = mapper.queryLogConfig();
//            logConfigs.forEach(System.out::println);
//        }catch (Exception e){
//            e.printStackTrace();
//        }

        Jedis jedis = new Jedis("10.122.238.97",16379);
        jedis.auth("XLhy!321YH");
        jedis.select(14);
        jedis.keys("*").forEach(System.out::println);
        jedis.hgetAll("wash:metric:20200531").entrySet().forEach(en-> System.out.println(en.getKey()+"->"+en.getValue()));
        jedis.close();

//        Pattern ConsumerTopicPatterns = Pattern.compile("^(bdt)\\w*");
//        System.out.println(ConsumerTopicPatterns.matcher("bdtest").matches());

    }
}
