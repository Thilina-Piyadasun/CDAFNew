package org.abithana.prescription;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.abithana.prescriptionBeans.PresDataBean;
import org.abithana.prescriptionBeans.PrescriptionDataBean;
import org.abithana.utill.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Vector;

/**
 * Created by Thilina on 12/28/2016.
 */
public class PrescriptionData implements Serializable{

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


        double d1=  getMaxHorizontalDistance(maxLat,minLat,maxLon,minLon);
        double d2 = getMaxVerticalDistance(maxLat,minLat,maxLon,minLon);

        N1=calcGridSize(d1);
        N2=calcGridSize(d2);
        System.out.println("=====================N1 SIZE============================");
        System.out.println(N1);
        System.out.println("=====================N2 SIZE============================");
        System.out.println(N2);
        dLat=corrdinateGap(maxLat,minLat,N1);
        dLon=corrdinateGap(maxLon,minLon,N2);

        DataFrame df=instance.getSqlContext().sql("Select * from " +tableName);

        List<PrescriptionDataBean> prescritptionDataJavaRDD = df.javaRDD().map(new Function<Row, PrescriptionDataBean>() {
            public PrescriptionDataBean call(Row row) {

                int weight=getWeightForCategory(row.getString(0));
                double lat=row.getDouble(1);
                double lon=row.getDouble(2);

                int x=(int)((lat-minLat)/dLat);
                int y=(int)((lon-minLon)/dLon);

                double latmid=minLat+x*dLat+dLat/2;
                double lonMid=minLon+y*dLon+dLon/2;

                int tractID=y*N1+x;

                PrescriptionDataBean prescritptionData=new PrescriptionDataBean(weight,lat,lon,tractID);
                return prescritptionData;
            }
        }).collect();

        DataFrame dataFrame = Config.getInstance().getSqlContext().createDataFrame(prescritptionDataJavaRDD, PrescriptionDataBean.class);
        dataFrame.registerTempTable(tableName);
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

        double cordianteGap=(Max-min)/n;
        return cordianteGap;
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

    public void digitizeMap2(String tableName){

        instance.getSqlContext().sql("show tables").show(20);

        double minLat=getMaxMinLatLonValues("min", lat, tableName);
        double maxLat=getMaxMinLatLonValues("max", lat, tableName);
        double minLon=getMaxMinLatLonValues("min", lon, tableName);
        double maxLon=getMaxMinLatLonValues("max", lon, tableName);


        double d1=  getMaxHorizontalDistance(maxLat,minLat,maxLon,minLon);
        double d2 = getMaxVerticalDistance(maxLat,minLat,maxLon,minLon);

        N1=calcGridSize(d1);
        N2=calcGridSize(d2);
        System.out.println("=====================N1 SIZE============================");
        System.out.println(N1);
        System.out.println("=====================N2 SIZE============================");
        System.out.println(N2);
        dLat=corrdinateGap(maxLat,minLat,N1);
        dLon=corrdinateGap(maxLon,minLon,N2);

        DataFrame df=instance.getSqlContext().sql("Select * from " +tableName);

        JavaRDD<String> prescritptionDataJavaRDD = df.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {

                String prescritptionData=null;
                try {
                    double lat = row.getDouble(2);
                    double lon = row.getDouble(1);

                    String sURL = "http://data.fcc.gov/api/block/find?format=json&latitude=" + lat + "&longitude=" + lon + "&showall=true";


                    // Connect to the URL using java's native library
                    URL url = new URL(sURL);
                    HttpURLConnection request = (HttpURLConnection) url.openConnection();
                    request.connect();

                    // Convert to a JSON object to print data
                    JsonParser jp = new JsonParser(); //from gson
                    JsonElement root = jp.parse(new InputStreamReader((InputStream) request.getContent())); //Convert the input stream to a json element
                    JsonObject rootobj = root.getAsJsonObject(); //May be an array, may be an object.
                    //System.out.println(rootobj.toString());
                   prescritptionData=rootobj.toString();

                } catch (Exception e) {
                    e.printStackTrace();
                }


                return prescritptionData;
            }
        }).cache();

        prescritptionDataJavaRDD.saveAsTextFile("D:\\FYP\\Ctract\\mytracts.txt");


    }
}
