package joining.pojo;

/**
 * @ClassName Stream1
 * @Author lihuasong
 * @Description
 * @Date 2021-10-18 20:24
 * @Version V1.0
 **/

public class Stream1 {
    public String id;
    public String name;


    public Stream1() {
    }

    public Stream1(String id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Stream1{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
