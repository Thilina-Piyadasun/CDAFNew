package org.abithana.beans;

import java.io.Serializable;

/**
 * Created by Thilina on 12/16/2016.
 */
public class CrimeTestBeanWithTIme implements Serializable {


    private int year;
    private int Time;
    private String DayOfWeek;
    private String PdDistrict;
    private String Resolution;
    private double X;
    private double Y;

    public CrimeTestBeanWithTIme(int year,int time, String dayOfWeek, String pdDistrict,String resolution, double x, double y) {
        this.year=year;
        this.Time =time;
        this.DayOfWeek = dayOfWeek;
        this.PdDistrict = pdDistrict;
        this.Resolution=resolution;
        this.X = x;
        this.Y = y;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTime() {
        return Time;
    }

    public void setTime(int time) {
        Time = time;
    }

    public String getDayOfWeek() {
        return DayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        DayOfWeek = dayOfWeek;
    }

    public String getPdDistrict() {
        return PdDistrict;
    }

    public void setPdDistrict(String pdDistrict) {
        PdDistrict = pdDistrict;
    }

    public String getResolution() {
        return Resolution;
    }

    public void setResolution(String resolution) {
        Resolution = resolution;
    }

    public double getX() {
        return X;
    }

    public void setX(double x) {
        X = x;
    }

    public double getY() {
        return Y;
    }

    public void setY(double y) {
        Y = y;
    }
}
