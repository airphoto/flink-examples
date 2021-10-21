package example.java.state.p8_window.utils;

/**
 * @ClassName WindowCountEvent
 * @Author lihuasong
 * @Description
 *
 * @Date 2021-10-19 20:15
 * @Version V1.0
 **/

public class WindowCountEvent {
    private Long timestamp;
    private String word;
    private Long value;


    @Override
    public String toString() {
        return "WindowCountEvent{" +
                "timestamp=" + timestamp +
                ", word='" + word + '\'' +
                ", value=" + value +
                '}';
    }


    public WindowCountEvent() {
    }

    public WindowCountEvent(Long timestamp, String word, Long value) {
        this.timestamp = timestamp;
        this.word = word;
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
