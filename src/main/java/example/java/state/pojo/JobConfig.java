package example.java.state.pojo;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/15
 **/
public class JobConfig {

    private String key;
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
