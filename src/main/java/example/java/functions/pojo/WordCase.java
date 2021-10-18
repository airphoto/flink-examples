package example.java.functions.pojo;

/**
 * @ClassNameWordCase
 * @Description
 *
 *      测试的POJO类
 *
 *      当出现以下情况的时候，不能成为key：
 *      1、他是POJO类，但是不覆盖hashCode()方法
 *      2：他是数组类型
 *
 * @Author lihuasong
 * @Date2020/4/18 18:28
 * @Version V1.0
 **/
public class WordCase {

    private String key;
    private String field;
    private Integer value;

    public WordCase() {
    }

    public WordCase(String key, String field, Integer value) {
        this.key = key;
        this.field = field;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "WordCase{" +
                "key='" + key + '\'' +
                ", field='" + field + '\'' +
                ", value=" + value +
                '}';
    }
}
