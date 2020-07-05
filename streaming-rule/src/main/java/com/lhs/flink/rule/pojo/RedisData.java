package com.lhs.flink.rule.pojo;

/**
 * @ClassNameRedisData
 * @Description
 * @Author lihuasong
 * @Date2020/7/4 8:58
 * @Version V1.0
 **/
public class RedisData {
    private int db;
    private int redisType;
    private String key;
    private String field;
    private String value;
    private Integer ttl;

    public RedisData() {
    }

    public RedisData(int db, int redisType, String key, String field, String value, Integer ttl) {
        this.db = db;
        this.redisType = redisType;
        this.key = key;
        this.field = field;
        this.value = value;
        this.ttl = ttl;
    }

    public int getDb() {
        return db;
    }

    public void setDb(int db) {
        this.db = db;
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    public int getRedisType() {
        return redisType;
    }

    public void setRedisType(int redisType) {
        this.redisType = redisType;
    }

    @Override
    public String toString() {
        return "RedisData{" +
                "db=" + db +
                ", redisType=" + redisType +
                ", key='" + key + '\'' +
                ", field='" + field + '\'' +
                ", value='" + value + '\'' +
                ", ttl=" + ttl +
                '}';
    }
}
