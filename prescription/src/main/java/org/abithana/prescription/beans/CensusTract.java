package org.abithana.prescription.beans;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by malakaganga on 1/23/17.
 */
public class CensusTract implements Serializable {
    private ArrayList<Double> polygonLatPoints = new ArrayList<Double>();
    private ArrayList<Double> polygonLonPoints = new ArrayList<Double>();
    private long censusId;
    private double midLatitude;
    private double midLongitude;

    public double getMidLatitude() {
        return midLatitude;
    }

    public void setMidLatitude(double midLatitude) {
        this.midLatitude = midLatitude;
    }

    public double getMidLongitude() {
        return midLongitude;
    }

    public void setMidLongitude(double midLongitude) {
        this.midLongitude = midLongitude;
    }

    public ArrayList<Double> getPolygonLatPoints() {
        return polygonLatPoints;
    }

    public void setPolygonLatPoints(ArrayList<Double> polygonLatPoints) {
        this.polygonLatPoints = polygonLatPoints;
    }

    public ArrayList<Double> getPolygonLonPoints() {
        return polygonLonPoints;
    }

    public void setPolygonLonPoints(ArrayList<Double> polygonLonPoints) {
        this.polygonLonPoints = polygonLonPoints;
    }

    public long getCensusId() {
        return censusId;
    }

    public void setCensusId(long censusId) {
        this.censusId = censusId;
    }
}
