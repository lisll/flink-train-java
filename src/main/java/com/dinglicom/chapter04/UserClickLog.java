package com.dinglicom.chapter04;

import java.io.Serializable;

/**
 * @author ly
 * @Date Create in 14:59 2021/3/3 0003
 * @Description
 */
public class UserClickLog implements Serializable {
    public String userID;
    public String eventTime;
    public String eventType;
    public String pageID;

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setPageID(String pageID) {
        this.pageID = pageID;
    }

    public String getUserID() {
        return userID;
    }

    public String getEventTime() {
        return eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPageID() {
        return pageID;
    }

    @Override
    public String toString() {
        return "UserClickLog{" +
                "userID='" + userID + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", eventType='" + eventType + '\'' +
                ", pageID='" + pageID + '\'' +
                '}';
    }
}
