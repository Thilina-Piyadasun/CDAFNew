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
    private String Resolution;
    private double X;
    private double Y;

    public CrimeDataBeanWithTime(int year,int time, String category, String dayOfWeek, String pdDistrict,String resolution, double x, double y) {
        this.year=year;
        this.Time =time;
        this.Category = category;
        this.DayOfWeek = dayOfWeek;
        this.PdDistrict = pdDistrict;
        this.Resolution=resolution;
        this.X = x;
        this.Y = y;
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

    public double getX() {
        return X;
    }

    public void setX(double x) {
        this.X = x;
    }

    public double getY() {
        return Y;
    }

    public void setY(double y) {
        this.Y = y;
    }

    public void setTime(int time) {
        Time = time;
    }

    public void setResolution(String resolution) {
        Resolution = resolution;
    }

    public int getTime() {
        return Time;
    }

    public String getResolution() {
        return Resolution;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }
}
