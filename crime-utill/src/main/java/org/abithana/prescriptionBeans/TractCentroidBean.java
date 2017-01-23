package org.abithana.prescriptionBeans;

import java.io.Serializable;

/**
 * Created by Thilina on 1/4/2017.
 */
public class TractCentroidBean implements Serializable {

    private double lat;
    private double lon;
    private long tractID;
    private int work;

    public TractCentroidBean(double lat, double lon, long tractID) {
        this.lat = lat;
        this.lon = lon;
        this.tractID = tractID;
    }

    public TractCentroidBean(double lat, double lon, long tractID, int work) {
        this.lat = lat;
        this.lon = lon;
        this.tractID = tractID;
        this.work = work;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public long getTractID() {
        return tractID;
    }

    public void setTractID(long tractID) {
        this.tractID = tractID;
    }

    public int getWork() {
        return work;
    }

    public void setWork(int work) {
        this.work = work;
    }
}
