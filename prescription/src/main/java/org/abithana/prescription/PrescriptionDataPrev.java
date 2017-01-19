package org.abithana.prescription;

import org.abithana.prescriptionBeans.PresDataBean;
import org.abithana.utill.Config;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

/**
 * Created by Thilina on 12/28/2016.
 */
public class PrescriptionDataPrev implements Serializable{

    private final int DISTANCE_GAP_IN_METERS=500; //in meters

    private DataFrame prescriptionDs;
    private String lat;
    private String lon;
    private String category;
    private Config instance=Config.getInstance();
    private int N1;
    private int N2;



    private double dLat;
    private double dLon;

    public DataFrame createPrescriptionDs(String name,String datasetUsing){


        DataFrame df=instance.getSqlContext().sql("Select "+category+","+lat+","+lon+" from "+datasetUsing);
        df=integrateTractID(df);
        df.registerTempTable(name);
        return df;
    }
    public DataFrame integrateTractID(DataFrame df){
        return df;
    }
    public DataFrame digitizeMap(String tableName){

        instance.getSqlContext().sql("show tables").show(20);

        double minLat=getMaxMinLatLonValues("min", lat, tableName);
        double maxLat=getMaxMinLatLonValues("max", lat, tableName);
        double minLon=getMaxMinLatLonValues("min", lon, tableName);
        double maxLon=getMaxMinLatLonValues("max", lon, tableName);

        System.out.println("===================== MIN LATITUDE============================");
        System.out.println(minLat);
        System.out.println("=====================MAX LATITUDE============================");
        System.out.println(maxLat);

        System.out.println("===================== MIN LONGITUDE============================");
        System.out.println(minLon);
        System.out.println("=====================MAX LONGITUDE============================");
        System.out.println(maxLon);


        double d1=  getMaxHorizontalDistance(maxLat,minLat,maxLon,minLon);
        double d2 = getMaxVerticalDistance(maxLat,minLat,maxLon,minLon);

        System.out.println("=====================HORIZONTAL Distance============================");
        System.out.println(d1);
        System.out.println("=====================Vertical Distance============================");
        System.out.println(d2);

        N1=calcGridSize(d1);
        N2=calcGridSize(d2);

        System.out.println("=====================N1 SIZE============================");
        System.out.println(N1);
        System.out.println("=====================N2 SIZE============================");
        System.out.println(N2);
        dLat=corrdinateGap(maxLat,minLat,N1);
        dLon=corrdinateGap(maxLon,minLon,N2);
        System.out.println("=====================LATITUDE DISTANCE============================");
        System.out.println(dLat);
        System.out.println("=====================LONGITUDE DISTANCE============================");
        System.out.println(dLon);

        DataFrame df=instance.getSqlContext().sql("Select * from " +tableName);

        List<PresDataBean> prescritptionDataJavaRDD;
        prescritptionDataJavaRDD = df.javaRDD().map(row -> {

            int weight=getWeightForCategory(row.getString(0));
            double lat1 =row.getDouble(1);
            double lon1 =row.getDouble(2);

            int x=(int)((lat1 -minLat)/dLat);
            int y=(int)((lon1 -minLon)/dLon);

            double latmid=minLat+x*dLat+dLat/2;
            double lonMid=minLon+y*dLon+dLon/2;

            int tractID=y*N1+x;

            return new PresDataBean(weight, lat1, lon1,tractID,latmid,lonMid);
        }).collect();

        DataFrame dataFrame = Config.getInstance().getSqlContext().createDataFrame(prescritptionDataJavaRDD, PresDataBean.class);
        dataFrame.show(40);
        dataFrame.registerTempTable("mytbl");

        //DataFrame dataFrame2 = instance.getSqlContext().sql("select sum(categoryWeight) as weight,cell from mytbl group by cell order by cell");
        //DataFrame dataFrame2 = instance.getSqlContext().sql("select distinct cell from mytbl order by cell");
        List<Row> list = instance.getSqlContext().sql("select distinct tractID from mytbl order by cell").collectAsList();

        List<PresDataBean> missingCells=new Vector<>();


        for(int i=0;i<N1;i++){
            for (int j=0;j<N2;j++){

                double latmid=minLat+i*dLat+dLat/2;
                double lonMid=minLon+j*dLon+dLon/2;
                int index=j*N1+i;

                PresDataBean prescritptionData=new PresDataBean(0,latmid,lonMid,index,latmid,lonMid);
                missingCells.add(prescritptionData);
            }
        }

        DataFrame missingDF=Config.getInstance().getSqlContext().createDataFrame(missingCells,PresDataBean.class);
        DataFrame f=missingDF.unionAll(dataFrame);
        f.registerTempTable("newtbl");
        instance.getSqlContext().sql("select sum(categoryWeight) as weight,cell,cellMidLon,cellMidLat from newtbl group by cell,cellMidLon,cellMidLat order by cell").show(500);
        return dataFrame;
    }


    public double getMaxMinLatLonValues(String minorMax,String latOrLong,String tableName){

        String minMaxLatLong=minorMax+"("+latOrLong+")";
        instance.getSqlContext().sql("select "+minMaxLatLong+" from "+tableName).show(20);
        Row[] row=instance.getSqlContext().sql("select "+minMaxLatLong+" from "+tableName).collect();
        double d=0;
        try {
            d=row[0].getDouble(0);
        }catch (Exception e){
            e.printStackTrace();
        }
        return d;
    }

    public double getMaxHorizontalDistance(double maxLat,double minLat,double maxLong,double minLong){

        double val1=distanceInMeters(maxLat,maxLat,maxLong,minLong);
        double val2=distanceInMeters(minLat,minLat,maxLong,minLong);
        if(val1>=val2)
            return val1;
        else
            return val2;
    }

    public double getMaxVerticalDistance(double maxLat,double minLat,double maxLong,double minLong){

        double val1=distanceInMeters(maxLat,minLat,maxLong,maxLong);
        double val2=distanceInMeters(maxLat,minLat,minLong,minLong);
        if(val1>=val2)
            return val1;
        else
            return val2;
    }

    private double distanceInMeters(double lat1, double lat2, double lon1,
        double lon2) {

            final int R = 6371; // Radius of the earth

            Double latDistance = Math.toRadians(lat2 - lat1);
            Double lonDistance = Math.toRadians(lon2 - lon1);
            Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
            Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double distance = R * c * 1000; // convert to meters
            //return Math.sqrt(distance);
        return distance;
    }

    public int calcGridSize(double distance){

        int n=(int)(distance/DISTANCE_GAP_IN_METERS)+1;
        return n;
    }

    public double corrdinateGap(double Max,double min,int n){

        return (Max-min)/n;
    }
    public double getdLat() {
        return dLat;
    }

    public void setdLat(double dLat) {
        this.dLat = dLat;
    }

    public double getdLon() {
        return dLon;
    }

    public void setdLon(double dLon) {
        this.dLon = dLon;
    }
    public int getWeightForCategory(String category){
        return 1;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
