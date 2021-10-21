package async_io.pojo;

/**
 * @ClassName CategoryInfo
 * @Author lihuasong
 * @Description
 * @Date 2021-09-30 15:22
 * @Version V1.0
 **/

public class CategoryInfo {
    private String id;
    private String catetory;

    public CategoryInfo() {
    }

    public CategoryInfo(String id, String catetory) {
        this.id = id;
        this.catetory = catetory;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCatetory() {
        return catetory;
    }

    public void setCatetory(String catetory) {
        this.catetory = catetory;
    }

    @Override
    public String toString() {
        return "CategoryInfo{" +
                "id='" + id + '\'' +
                ", catetory='" + catetory + '\'' +
                '}';
    }
}
