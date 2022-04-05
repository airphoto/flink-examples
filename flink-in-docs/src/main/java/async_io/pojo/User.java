package async_io.pojo;

/**
 * @ClassName User
 * @Author lihuasong
 * @Description
 * @Date 2022/4/5 9:55
 * @Version V1.0
 **/
public class User {
    private Integer app_id;
    private String user_id;
    private String xl_id;

    public User() {
    }

    public User(Integer app_id, String user_id, String xl_id) {
        this.app_id = app_id;
        this.user_id = user_id;
        this.xl_id = xl_id;
    }

    public Integer getApp_id() {
        return app_id;
    }

    public void setApp_id(Integer app_id) {
        this.app_id = app_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getXl_id() {
        return xl_id;
    }

    public void setXl_id(String xl_id) {
        this.xl_id = xl_id;
    }

    @Override
    public String toString() {
        return "User{" +
                "app_id=" + app_id +
                ", user_id='" + user_id + '\'' +
                ", xl_id='" + xl_id + '\'' +
                '}';
    }
}
