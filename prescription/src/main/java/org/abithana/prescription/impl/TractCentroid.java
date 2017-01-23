package org.abithana.prescription.impl;

import com.graphhopper.GraphHopper;
import org.abithana.prescriptionBeans.*;
import org.abithana.utill.Config;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by Thilina on 1/4/2017.
 */
public class TractCentroid implements Serializable{

    private int toalWork=0;
    static GraphHopper hopper=Routing.getRoute();
    Routing routing=new Routing();

    private Config instance=Config.getInstance();

    public List<TractCentroidBean> getAllTractCentroids(String tableName){

        List<Long> allTractIDList=getAllTractID(tableName);

        System.out.println("========================================================");
        System.out.println( "CALCULATING BEST CRIME CENTRODIS OF INDIVIDUAL TRACTS");
        System.out.println("========================================================");



        List<TractCentroidBean> bestLocationsOfTracts = new ArrayList<>();
        for(long tractID:allTractIDList){
            bestLocationsOfTracts.add(getBestLocation(tractID, tableName));
        }
        System.out.println("========================================================");
        System.out.println( "CALCULATING BEST CRIME CENTRODIS OF INDIVIDUAL TRACTS DONE");
        System.out.println("========================================================");



        System.out.println();
        for(TractCentroidBean tract:bestLocationsOfTracts){
            System.out.println(tract.getTractID() +" Lat: "+tract.getLat()+" lon: "+tract.getLon());
        }
        return bestLocationsOfTracts;
    }

    public List<Long> getAllTractID(String tableName){

        DataFrame df=instance.getSqlContext().sql("select tractID from "+tableName).distinct();

        List<Long> tractIDList = df.javaRDD().map(new Function<Row, Long>() {
            public Long call(Row row) {

                long tractID=row.getAs("tractID");
                return tractID;
            }
        }).collect();

        return tractIDList;
    }

    public TractCentroidBean getBestLocation(long tractID,String tableName){

        System.out.println("enter");
        DataFrame df=instance.getSqlContext().sql("select categoryWeight,lat,lon from "+tableName +" where tractID="+tractID);

        List<LocationWeightBean> locations = df.javaRDD().map(new Function<Row, LocationWeightBean>() {
            public LocationWeightBean call(Row row) {

                LocationWeightBean locationWeightBean=new LocationWeightBean(row.getAs("categoryWeight"),row.getAs("lat"),row.getAs("lon"));
                return locationWeightBean;
            }
        }).collect();
        int size=locations.size();
        int[][] weightMatrix=new int[size][size];

        System.out.println("Tract :"+ tractID+" No of crime locations : "+size+ " .Calculating distance between each pair...");
        long time_1 = System.currentTimeMillis();
        IntStream.range(0, size).parallel().forEach(i->
        {
            for(int j=i;j<size;j++){

                if(i==j){
                    weightMatrix[i][j]=0;
                }
                else {
                    double x1=locations.get(i).getLat();
                    double y1=locations.get(i).getLon();
                    double x2=locations.get(j).getLat();
                    double y2=locations.get(j).getLon();
                    /*double x1 = 37.7745985956747;
                    double y1 = -122.425891675136;
                    double x2 =37.8008726327692;
                    double y2 = -122.426995326765;*/
                    int distance=calcRoadDistance(x1,y1,x2,y2);
                    weightMatrix[i][j]=distance*locations.get(j).getCategoryWeight();
                    weightMatrix[j][i]=distance*locations.get(i).getCategoryWeight();
                }
            }
        });

        long time_2 = System.currentTimeMillis();

/*        System.out.println("====================Estimated time============================");
        System.out.println(time_2-time_1)*/;

        int minWork=Integer.MAX_VALUE;
        int bestIndex=0;

        //System.out.println("caculating best index for tractID" +tractID);
        for(int i=0;i<size;i++){

            int sum=0;
            for(int j=0;j<size;j++){
                sum=sum+weightMatrix[i][j];
            }
            if(sum<minWork){
                minWork=sum;
                bestIndex=i;
            }

        }

        incrementTotalWork(minWork);
        return new TractCentroidBean(locations.get(bestIndex).getLat(),locations.get(bestIndex).getLon(),tractID,minWork);

    }

    public void incrementTotalWork(int val){
        toalWork=toalWork+val;
    }

    public int getToalWork() {
        return toalWork;
    }

    public int calcRoadDistance( Double latFrom, Double  lonFrom, Double latTo, Double lonTo ) {

        return routing.calc( hopper,latFrom, lonFrom, latTo, lonTo);
    }

}
