package example.java.cep.p1_example;

import java.util.Objects;

/**
 * @ClassName SingleEvent
 * @Author lihuasong
 * @Description
 * @Date 2021-10-25 19:39
 * @Version V1.0
 **/

public class SingleEvent {
    private String id;
    private String eventType;
    private Long eventTime;

    public SingleEvent() {
    }

    public SingleEvent(String id, String eventType, Long eventTime) {
        this.id = id;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleEvent that = (SingleEvent) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(eventType, that.eventType) &&
                Objects.equals(eventTime, that.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, eventType, eventTime);
    }
}
