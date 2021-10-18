package example.java.sql.udf.source;

import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassNameMySystemConnector
 * @Description
 * @Author lihuasong
 * @Date2020/4/25 18:20
 * @Version V1.0
 **/
public class MySystemConnector extends ConnectorDescriptor {
    public final boolean isDebug;

    public MySystemConnector(boolean isDebug) {
        super("my-system", 1, false);
        this.isDebug = isDebug;
    }
    @Override
    protected Map<String, String> toConnectorProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.debug", Boolean.toString(isDebug));
        return properties;
    }
}
