package joining.pojo;

/**
 * @ClassName Stream2
 * @Author lihuasong
 * @Description
 * @Date 2021-10-18 20:25
 * @Version V1.0
 **/

public class Stream2 {
    public String id;
    public String value;

    public Stream2() {
    }

    public Stream2(String id, String value) {
        this.id = id;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Stream2{" +
                "id='" + id + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
