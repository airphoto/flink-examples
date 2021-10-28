package example.java.cep.examples.p2_doors;

import java.io.Serializable;
import java.util.Objects;

/**
 * @ClassName AccessEvent
 * @Author lihuasong
 * @Description
 *      刷卡访问事件的实体类对象
 *
 *      id:           自增id
 *      doorid:       门id
 *      doorStatus：  进出门的状态
 *      eventType
 *      employeeSysNo 员工号
 *      dateTime      事件时间
 *
 *      数据源格式:
 *      id,doorid,status,eventType,userid,datetime
 *      1,1,in,client,1,1635332817000
 * @Date 2021-10-27 18:32
 * @Version V1.0
 **/

public class AccessEvent implements Serializable {

    private Integer id;
    private Integer doorId;
    private String doorStatus;
    private Integer eventType;
    private String employeeSysNo;
    private String dateTime;


    @Override
    public String toString() {
        return "AccessEvent{" +
                "id=" + id +
                ", doorId=" + doorId +
                ", doorStatus='" + doorStatus + '\'' +
                ", eventType=" + eventType +
                ", employeeSysNo='" + employeeSysNo + '\'' +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccessEvent that = (AccessEvent) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(doorId, that.doorId) &&
                Objects.equals(doorStatus, that.doorStatus) &&
                Objects.equals(eventType, that.eventType) &&
                Objects.equals(employeeSysNo, that.employeeSysNo) &&
                Objects.equals(dateTime, that.dateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, doorId, doorStatus, eventType, employeeSysNo, dateTime);
    }

    public AccessEvent() {
    }

    public AccessEvent(Integer id, Integer doorId, String doorStatus, Integer eventType, String employeeSysNo, String dateTime) {
        this.id = id;
        this.doorId = doorId;
        this.doorStatus = doorStatus;
        this.eventType = eventType;
        this.employeeSysNo = employeeSysNo;
        this.dateTime = dateTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getDoorId() {
        return doorId;
    }

    public void setDoorId(Integer doorId) {
        this.doorId = doorId;
    }

    public String getDoorStatus() {
        return doorStatus;
    }

    public void setDoorStatus(String doorStatus) {
        this.doorStatus = doorStatus;
    }

    public Integer getEventType() {
        return eventType;
    }

    public void setEventType(Integer eventType) {
        this.eventType = eventType;
    }

    public String getEmployeeSysNo() {
        return employeeSysNo;
    }

    public void setEmployeeSysNo(String employeeSysNo) {
        this.employeeSysNo = employeeSysNo;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }
}
