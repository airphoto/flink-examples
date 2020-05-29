import com.lhs.flink.dao.LogConfigMapper;
import com.lhs.flink.dao.MybatisSessionFactory;
import com.lhs.flink.pojo.LogConfig;
import org.apache.ibatis.session.SqlSession;

import java.util.List;
import java.util.Optional;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/27
 **/
public class MyBatisTest {
    public static void main(String[] args) throws InterruptedException {
//        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
//        LogConfigMapper mapper = sqlSession.getMapper(LogConfigMapper.class);
//        while (true) {
//            List<LogConfig> logConfigs = mapper.queryLogConfig();
//            Optional<String> reduce = logConfigs.stream().map(LogConfig::toString).reduce((left, right) -> left + "," + right);
//            System.out.println("log_configs : ["+reduce.get()+"]");
//            Thread.sleep(500L);
//        }
    }
}
