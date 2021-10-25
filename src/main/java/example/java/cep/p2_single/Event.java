package example.java.cep.p2_single;

import java.util.Objects;

/**
 * @ClassName Event
 * @Author lihuasong
 * @Description
 * @Date 2021-10-25 20:41
 * @Version V1.0
 **/

public class Event {
    private String userId;
    private String eventName;
    private String eventTime;

    public Event() {
    }

    public Event(String userId, String eventName, String eventTime) {
        this.userId = userId;
        this.eventName = eventName;
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(userId, event.userId) &&
                Objects.equals(eventName, event.eventName) &&
                Objects.equals(eventTime, event.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, eventName, eventTime);
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }
}
