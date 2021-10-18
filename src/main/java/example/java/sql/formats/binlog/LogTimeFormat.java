package example.java.sql.formats.binlog;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/30
 **/
public class LogTimeFormat {

    /** Formatter for RFC 3339-compliant string representation of a time value. */
    static final DateTimeFormatter RFC3339_TIME_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendPattern("'Z'")
            .toFormatter();

    /** Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC timezone). */
    static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .append(RFC3339_TIME_FORMAT)
            .toFormatter();

    private LogTimeFormat() {
    }
}
