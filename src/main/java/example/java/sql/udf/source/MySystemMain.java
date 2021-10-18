package example.java.sql.udf.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassNameMySystemMain
 * @Description
 * @Author lihuasong
 * @Date2020/4/25 18:21
 * @Version V1.0
 **/
public class MySystemMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(environment);

        tabEnv.connect(
                new MySystemConnector(true)
        )
                .withSchema(new Schema().field("key", DataTypes.STRING()))
                .inAppendMode()
                .createTemporaryTable("test");


        Table table = tabEnv.sqlQuery("select * from test");

        tabEnv.toAppendStream(table, Row.class);

        tabEnv.execute("test");

    }

}
