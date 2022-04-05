package async_io.pojo;

/**
 * @ClassName UserChange
 * @Author lihuasong
 * @Description
 * @Date 2022/4/5 10:05
 * @Version V1.0
 **/
public class UserChange {
    private Integer appId;
    private String xlId;
    private String field;
    private String value;
    private String fieldType;

    public UserChange() {
    }

    public UserChange(Integer appId, String xlId, String field, String value,String fieldType) {
        this.appId = appId;
        this.xlId = xlId;
        this.field = field;
        this.value = value;
        this.fieldType = fieldType;
    }

    @Override
    public String toString() {
        return "UserChange{" +
                "appId=" + appId +
                ", xlId='" + xlId + '\'' +
                ", field='" + field + '\'' +
                ", value='" + value + '\'' +
                ", fieldType='" + fieldType + '\'' +
                '}';
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public String getXlId() {
        return xlId;
    }

    public void setXlId(String xlId) {
        this.xlId = xlId;
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

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }
}
