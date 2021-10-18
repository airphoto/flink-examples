package example.java.sql.pojo;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/21
 **/
public class RedisCase {

    private String k;
    private String f;
    private String v;

    public RedisCase() {
    }

    public RedisCase(String key, String fied, String value) {
        this.k = key;
        this.f = fied;
        this.v = value;
    }

    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    public String getF() {
        return f;
    }

    public void setF(String f) {
        this.f = f;
    }

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }
}
