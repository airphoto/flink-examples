package example.java.sql.p1_sources_sinks.redis.connector;

import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/22
 **/
public class RedisConnector extends ConnectorDescriptor{


    public RedisConnector(String type, int version, boolean formatNeeded) {
        super(type, version, formatNeeded);
        System.out.println("RedisConnector");
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        System.out.println("toConnectorProperties");
        Map<String,String> map = new HashMap<>();
        map.put("connector.debug","false");
        return map;
    }
}
