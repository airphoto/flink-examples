package joining.pojo;

/**
 * @ClassName JoinStream
 * @Author lihuasong
 * @Description
 * @Date 2021-10-18 20:35
 * @Version V1.0
 **/

public class JoinStream {
    public String name;
    public String value;

    public JoinStream() {
    }

    public JoinStream(String name, String value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String toString() {
        return "JoinStream{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
