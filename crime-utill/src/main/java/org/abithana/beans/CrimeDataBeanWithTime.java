package org.abithana.beans;

import java.io.Serializable;

/**
 * Created by Thilina on 8/17/2016.
 */
public class CrimeDataBeanWithTime implements Serializable {

    private int year;
    private int Time;
    private String DayOfWeek;
    private String Category;
    private String PdDistrict;
    private String resolution;
    private double latitude;
    private double longitude;

    public CrimeDataBeanWithTime(int year,int time, String category, String dayOfWeek, String pdDistrict,String resolution, double x, double y) {
        this.year=year;
        this.Time =time;
        this.Category = category;
        this.DayOfWeek = dayOfWeek;
        this.PdDistrict = pdDistrict;
        this.resolution =resolution;
        this.latitude = x;
        this.longitude = y;
    }

    public String getDayOfWeek() {
        return DayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.DayOfWeek = dayOfWeek;
    }

    public String getCategory() {
        return Category;
    }

    public void setCategory(String category) {
        this.Category = category;
    }

    public String getPdDistrict() {
        return PdDistrict;
    }

    public void setPdDistrict(String pdDistrict) {
        this.PdDistrict = pdDistrict;
    }

    public void setTime(int time) {
        Time = time;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public int getTime() {
        return Time;
    }

    public String getResolution() {
        return resolution;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}
