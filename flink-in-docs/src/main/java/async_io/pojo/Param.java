package async_io.pojo;

/**
 * @ClassName Param
 * @Author lihuasong
 * @Description
 * @Date 2022/4/5 9:57
 * @Version V1.0
 **/
public class Param {
    private String field_name;
    private String field_value;
    private String change_type;

    public Param() {
    }

    public Param(String field_name, String field_value, String change_type) {
        this.field_name = field_name;
        this.field_value = field_value;
        this.change_type = change_type;
    }

    @Override
    public String toString() {
        return "Param{" +
                "field_name='" + field_name + '\'' +
                ", field_value='" + field_value + '\'' +
                ", change_type='" + change_type + '\'' +
                '}';
    }

    public String getField_name() {
        return field_name;
    }

    public void setField_name(String field_name) {
        this.field_name = field_name;
    }

    public String getField_value() {
        return field_value;
    }

    public void setField_value(String field_value) {
        this.field_value = field_value;
    }

    public String getChange_type() {
        return change_type;
    }

    public void setChange_type(String change_type) {
        this.change_type = change_type;
    }
}
