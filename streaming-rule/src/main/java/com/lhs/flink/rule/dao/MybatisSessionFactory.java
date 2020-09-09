package com.lhs.flink.rule.dao;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * @author lihuasong
 * @description
 *      描述：mybatis工厂
 * @create 2020/5/26
 **/
public class MybatisSessionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MybatisSessionFactory.class);
    private static SqlSessionFactory sqlSessionFactory;

    private MybatisSessionFactory() {
    }

    static{
        InputStream inputStream=null;
        try{
            inputStream = MybatisSessionFactory.class.getClassLoader().getResourceAsStream("mybatis-config.xml");
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        } catch (Exception e){
            LOG.error("create MybatisSessionFactory read mybatis-config.xml cause Exception",e);
        } finally {
            if(inputStream != null){
                try {
                    inputStream.close();
                }catch (Exception e){
                    LOG.error("input stream close error",e);
                }
            }
        }
    }

    public static SqlSession getSession(){
        SqlSession sqlSession = null;
        if (sqlSessionFactory!=null){
            sqlSession = sqlSessionFactory.openSession();
        }
        return sqlSession;
    }

    public static void closeSession(SqlSession sqlSession){
        if (sqlSession!=null){
            try{
                sqlSession.close();
            }catch (Exception e){
                LOG.error("sql session close error",e);
            }
        }
    }
}
