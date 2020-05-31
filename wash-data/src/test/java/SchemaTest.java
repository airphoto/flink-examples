import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/27
 **/
public class SchemaTest {
    public static void main(String[] args) {
        SchemaLoader schemaLoader = SchemaLoader.builder().schemaJson(new JSONObject("")).build();
        Schema build = schemaLoader.load().build();
        build.validate(new JSONObject());
    }
}
