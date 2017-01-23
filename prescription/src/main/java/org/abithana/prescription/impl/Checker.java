package org.abithana.prescription.impl;

import org.abithana.prescription.beans.CensusTract;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by malakaganga on 1/23/17.
 */
public class Checker implements Serializable{

    private ArrayList<CensusTract> censusTracts = new ArrayList<CensusTract>(); // to hold census tracts
    private HashMap<Long, CensusTract> censusMap = new HashMap<Long, CensusTract>();

    public Checker() {
        extractDataFromBoundry();
    }

    public long polygonChecker(double[] point) {


        long polygonID = 0;
        double pointLong = point[0];
        double pointLat = point[1];

        for (CensusTract censusTract : censusTracts) {

            int count = 0;
            ArrayList<Double> latPoints = censusTract.getPolygonLatPoints();
            ArrayList<Double> longPoints = censusTract.getPolygonLonPoints();

            for (int i = 0; i < latPoints.size(); i++) {
                int j = (i + 1) % latPoints.size();
                double latUp = latPoints.get(i);
                double longUp = longPoints.get(i);
                double latDown = latPoints.get(j);
                double longDown = longPoints.get(j);

                if (latUp == latDown) {
                    continue;
                } else {
                    double upPoint = (latUp > latDown)? latUp : latDown;
                    double lowPoint = (latDown < latUp)? latDown : latUp;
                    if (pointLat > upPoint || pointLat < lowPoint) {
                        continue;
                    }
                    if (pointLong <= breakLong(latUp,longUp,latDown,longDown,pointLat)) {
                        count ++;
                    }
                }

            }
            if ((count % 2) == 1 ) {
                return censusTract.getCensusId();
            }


        }

        return polygonID;
    }
    private double breakLong(double latUp, double longUp, double latDown, double longDown, double pointLat) {
        double breakLong = longUp - ((latUp - pointLat)*(longUp - longDown))/(latUp - latDown);
        return breakLong;
    }

    private void extractDataFromBoundry() {

        CsvReader csvReader = new CsvReader(); // to read the Census_2010_Tracts.csv file
        Integer[] wantedColumns = {2, 4, 11, 12}; // indicated wanted columns to read
        List<String[]> allRows = csvReader.readCsv("./Census_2010_Tracts.csv", wantedColumns);


        for (int i = 1; i < allRows.size(); i++) {

            String[] columns = allRows.get(i);
            //Define two array lists to keep latitudes and longitudes of each boundary point
            ArrayList<Double> latitudes = new ArrayList<Double>();
            ArrayList<Double> longitudes = new ArrayList<Double>();

            // to keep id of cencus tract
            long id = 0;

            //New census tract object
            CensusTract censusTract = new CensusTract();

            //To keep mid points
            double midLong = 0, midLat = 0;

            for (int j = 0; j < columns.length; j++) {

                if (j == 0) { // Boundary points column

                    String points = columns[j].substring(16, columns[j].length() - 3); // get the string with lats
                    // and longs

                    String[] individualPoints = points.split(", "); // get individual points into a string array

                    for (int k = 0; k < individualPoints.length; k++) {

                        String[] latNlong = individualPoints[k].split(" ");
                        longitudes.add(Double.parseDouble(latNlong[0]));
                        latitudes.add(Double.parseDouble(latNlong[1]));

                    }
                } else if (j >= columns.length - 2) {
                    double point = Double.parseDouble(columns[j]);
                    if (point < 0) {
                        midLong = point;
                    } else {
                        midLat = point;
                    }
                } else {
                    id = Long.parseLong(columns[j]);
                }

            }
            //Add details to the created census tract
            censusTract.setCensusId(id);
            censusTract.setMidLongitude(midLong);
            censusTract.setMidLatitude(midLat);
            censusTract.setPolygonLonPoints(longitudes);
            censusTract.setPolygonLatPoints(latitudes);

            //add census tract to list of tracts and map of tracts

            censusTracts.add(censusTract);
            censusMap.put(id, censusTract);


        }
    }
}
