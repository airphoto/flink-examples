package async_io.pojo;

import java.util.List;

/**
 * @ClassName UserProfile
 * @Author lihuasong
 * @Description
 * @Date 2022/4/5 9:54
 * @Version V1.0
 **/
public class UserProfile {
    private String type;
    private String event_type;
    private User user;
    private List<Param> param;

    public UserProfile() {
    }

    public UserProfile(String type, String event_type, User user, List<Param> param) {
        this.type = type;
        this.event_type = event_type;
        this.user = user;
        this.param = param;
    }

    @Override
    public String toString() {
        return "UserProfile{" +
                "type='" + type + '\'' +
                ", event_type='" + event_type + '\'' +
                ", user=" + user +
                ", param=" + param +
                '}';
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public List<Param> getParam() {
        return param;
    }

    public void setParam(List<Param> param) {
        this.param = param;
    }
}
