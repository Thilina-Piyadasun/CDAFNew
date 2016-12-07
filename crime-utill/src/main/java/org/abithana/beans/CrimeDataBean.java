package org.abithana.beans;

import org.apache.commons.net.ntp.TimeStamp;

import java.sql.Timestamp;

/**
 * Created by acer on 11/20/2016.
 */
public class CrimeDataBean implements DataStoreBeans {

    private Timestamp time;
    private String dayOfWeek;
    private String category;
    private String pdDistrict;
    private double x;
    private double y;

    public CrimeDataBean(Timestamp time, String category,String dayOfWeek, String pdDistrict, double x, double y) {

        this.dayOfWeek = dayOfWeek;
        this.category = category;
        this.pdDistrict = pdDistrict;
        this.x = x;
        this.y = y;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getPdDistrict() {
        return pdDistrict;
    }

    public void setPdDistrict(String pdDistrict) {
        this.pdDistrict = pdDistrict;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }
}
