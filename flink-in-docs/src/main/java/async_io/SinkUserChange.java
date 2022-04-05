package async_io;

import async_io.pojo.UserChange;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName SinkUserChange
 * @Author lihuasong
 * @Description     这个二阶段提交有点异常
 * @Date 2022/4/5 16:21
 * @Version V1.0
 **/
public class SinkUserChange extends TwoPhaseCommitSinkFunction<UserChange,SinkUserChange.ConnectionState,Void> {

    public SinkUserChange() {
        super(new KryoSerializer<>(SinkUserChange.ConnectionState.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(ConnectionState transaction, UserChange value, Context context) throws Exception {
        System.out.println(value);
        Connection connection = transaction.connection;
        String sql = String.format("insert into test.user_%s(xl_id,%s) values(?,?) on duplicate key update %s=VALUES(%s)",value.getAppId(),value.getField(),value.getField(),value.getField());
        System.out.println(sql);
        System.out.println("------------------------");
        PreparedStatement pstm = connection.prepareStatement(sql);
        pstm.setString(1,value.getXlId());
        switch (value.getFieldType()){
            case "string" :{
                pstm.setString(2,value.getValue());
                break;
            }
            case "int" :{
                pstm.setInt(2,Integer.parseInt(value.getValue()));
                break;
            }
            default:{
                System.out.println("not correct");
                break;
            }
        }
        pstm.executeUpdate();
        pstm.close();
    }

    @Override
    protected ConnectionState beginTransaction() throws Exception {
        System.out.println("=================> begin transaction");
        Connection connection = DruidConnectionPool.getConnection();
        return new ConnectionState(connection);
    }

    @Override
    protected void preCommit(ConnectionState transaction) throws Exception {
        System.out.println("================> pre commit");
    }

    @Override
    protected void commit(ConnectionState transaction) {
        System.out.println("===============> commit");
        DruidConnectionPool.commit(transaction.connection);
//        Connection connection = transaction.connection;
//        try {
//            connection.commit();
//            connection.close();
//        } catch (SQLException e) {
//            throw new RuntimeException("提交事务异常");
//        }

    }

    @Override
    protected void abort(ConnectionState transaction) {
        System.out.println("===========> abort");
        DruidConnectionPool.rollback(transaction.connection);
//        Connection connection = transaction.connection;
//        try {
//            connection.rollback();
//            connection.close();
//        } catch (SQLException e) {
//            throw new RuntimeException("=========> 回滚");
//        }
    }

    public static class ConnectionState{
        private final transient Connection connection;
        public ConnectionState(Connection connection){
            this.connection = connection;
        }
    }
}
