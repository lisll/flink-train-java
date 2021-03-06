package com.dinglicom.chapter04;

import java.io.Serializable;

/**
 * @author ly
 * @Date Create in 15:00 2021/3/3 0003
 * @Description
 */
public class UserBrowseLog implements Serializable {
    public String userID;
    public String eventTime;
    public String eventType;
    public String productID;
    public Integer productPrice;


    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public void setProductPrice(int productPrice) {
        this.productPrice = productPrice;
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

    public String getProductID() {
        return productID;
    }

    public Integer getProductPrice() {
        return productPrice;
    }

    @Override
    public String toString() {
        return "UserBrowseLog{" +
                "userID='" + userID + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", eventType='" + eventType + '\'' +
                ", productID='" + productID + '\'' +
                ", productPrice=" + productPrice +
                '}';
    }
}
