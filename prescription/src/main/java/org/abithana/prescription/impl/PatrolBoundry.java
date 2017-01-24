package org.abithana.prescription.impl;

import com.graphhopper.GraphHopper;
import org.abithana.prescriptionBeans.LeaderBean;
import org.abithana.prescriptionBeans.TractCentroidBean;
import org.abithana.utill.Config;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Thilina on 1/5/2017.
 */
public class PatrolBoundry implements Serializable {

    private final double PI = Math.PI;
    double minDistanceApart=0.0;
    private int threashold;
    private String lat;
    private String lon;
    private Config instance=Config.getInstance();
    private List<LeaderBean> leaderList=new ArrayList<>();
    private List<TractCentroidBean> follwers=new ArrayList<>();
    static GraphHopper hopper=Routing.getRoute();
    Routing routing=new Routing();
    TractNeighbours tractNeighbours=TractNeighbours.getInstance();

    public List<LeaderBean> getLearders(List<TractCentroidBean> list,int noOfPatrols){

        String tableName="tractCentroidTable";
        DataFrame dataFrame=instance.getSqlContext().createDataFrame(list,TractCentroidBean.class);
        dataFrame.registerTempTable(tableName);
        double minLat=getMaxMinLatLonValues("min", "lat", tableName);
        double maxLat=getMaxMinLatLonValues("max", "lat", tableName);
        double minLon=getMaxMinLatLonValues("min", "lon", tableName);
        double maxLon=getMaxMinLatLonValues("max", "lon", tableName);


        double d1=  getMaxHorizontalDistance(maxLat,minLat,maxLon,minLon);
        double d2 = getMaxVerticalDistance(maxLat,minLat,maxLon,minLon);

        double recArea = d1 * d2;

        minDistanceApart= Math.sqrt(recArea/(noOfPatrols * PI));
        instance.getSqlContext().sql("select * from "+tableName+ " order by work desc").show(50);
        List<Row> workOrderList=instance.getSqlContext().sql("select * from "+tableName+ " order by work desc").collectAsList();

        for(Row row:workOrderList){
            if(leaderList.size()<noOfPatrols) {
                if (isMinDistanceApart(row.getAs("lat"), row.getAs("lon"))) {
                    LeaderBean leaderBean=new LeaderBean(row.getAs("lat"), row.getAs("lon"),row.getAs("tractID"),row.getAs("work"));
                    leaderList.add(leaderBean);
                }
                else {
                    TractCentroidBean centroidBean=new TractCentroidBean(row.getAs("lat"), row.getAs("lon"),row.getAs("tractID"),row.getAs("work"));
                    follwers.add(centroidBean);
                }
            }
            else {
                TractCentroidBean centroidBean=new TractCentroidBean(row.getAs("lat"), row.getAs("lon"),row.getAs("tractID"),row.getAs("work"));
                follwers.add(centroidBean);
            }
        }

        System.out.println("=================Intial Leader list SIZE IS =====================");
        System.out.println(leaderList.size());

        while (leaderList.size()<noOfPatrols){
            TractCentroidBean bean=follwers.remove(0);
            LeaderBean lb=new LeaderBean(bean.getLat(),bean.getLon(),bean.getTractID(),bean.getWork());
            leaderList.add(lb);
            System.out.println("add bean to leader list.New Size is "+leaderList.size() );
        }
        System.out.println("=================Follwer list SIZE IS =====================");
        System.out.println(follwers.size());

        instance.getSqlContext().dropTempTable(tableName);
        return leaderList;

    }
    public boolean isMinDistanceApart(double lat,double lon){
        if(leaderList.isEmpty()){
            return true;
        }
        else{
            for(LeaderBean leaderBean:leaderList){
                double distance=distanceInMeters(leaderBean.getLat(),lat,leaderBean.getLon(),lon);
                if(distance<minDistanceApart){
                    return false; //if one point is close by not consider it as a leader
                }
            }
        }
        return true;
    }
    private void seperateFolowers(List<TractCentroidBean> list){

        System.out.println("=========================Before remove==============");
        System.out.println( list.size());

        for(LeaderBean lb:leaderList){
            for(TractCentroidBean tb:list){
                if(lb.getLeaderTract()==tb.getTractID()){
                    list.remove(tb);
                    break;
                }
            }
        }
        System.out.println("=========================After remove==============");
        System.out.println( list.size());

        System.out.println("=========================Leaderlist Size==============");
        System.out.println( leaderList.size());

        for(LeaderBean l:leaderList){
            System.out.print(l.getLeaderTract() + "  ");
        }
        System.out.println();
        follwers=list;
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

    public void calcThreashold(int Totalwork,int n){
        //TODO method implement
        System.out.println("===============================");
        System.out.println("Total Work "+ Totalwork);
        threashold=Totalwork/n;
    }

    public void findPatrolBoundries(){

        System.out.println("========================================================");
        System.out.println( "AGGREGATING TRACTS TOGETHER");
        System.out.println("========================================================");

        for(TractCentroidBean centroidBean:follwers){

            int min=Integer.MAX_VALUE;
            int leader=-1;
            for(int i=0;i<leaderList.size();i++){
                LeaderBean leaderBean=leaderList.get(i);
               /* if(leaderBean.getLeaderWork()+centroidBean.getWork() < threashold*2){
                    int distance=calcRoadDistance(leaderBean.getLat(),leaderBean.getLon(),centroidBean.getLat(),centroidBean.getLon());
                    if(distance<min){
                        leader=i;
                        min=distance;
                    }
                }*/
                int distance=calcRoadDistance(leaderBean.getLat(),leaderBean.getLon(),centroidBean.getLat(),centroidBean.getLon());
                if(distance<min){
                    leader=i;
                    min=distance;
                }
            }
            leaderList.get(leader).addFollower(centroidBean.getTractID());
            leaderList.get(leader).incrementLeaderWork(centroidBean.getWork());

        }

        for(LeaderBean leaderBean:leaderList){
            System.out.println("================Leader "+leaderBean.getLeaderTract()+" Total leader work : "+leaderBean.getLeaderWork()+"=================");
            for(long i:leaderBean.getFollowers()){
                System.out.print(i + "  ");
            }
            System.out.println();
        }
    }

    public Map<Integer,List<Long>> getBoundryTractids()
    {
        Map<Integer,List<Long>> allset=new HashMap<>();
        try{
            int i=0;
            for(LeaderBean leaderBean:leaderList){
                List<Long> tractidList=new ArrayList<>();
                tractidList.addAll(leaderBean.getFollowers()) ;
                tractidList.add(leaderBean.getLeaderTract());
                allset.put(i,tractidList);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return allset;
    }
    /*
    * Check isNeighbour
    * */
    public boolean isNeighbour(LeaderBean leaderBean,long tractID){

        return leaderBean.getFollowers().contains(tractID);
    }
    public int calcRoadDistance(Double latFrom, Double  lonFrom, Double latTo, Double lonTo){
        return routing.calc( hopper,latFrom, lonFrom, latTo, lonTo);
    }
/*
* COllect all neighbours of current cluster lead by parameter leaderBean
* */
    public Set<Long> collectAllNeighbours(LeaderBean leaderBean){

        Set<Long> allNeighbours=new HashSet<>();

        allNeighbours.addAll(getNeighbours(leaderBean.getLeaderTract()));

        for(long i:leaderBean.getFollowers()){
            allNeighbours.addAll(getNeighbours(i));
        }
        return allNeighbours;

    }

    public Set<Long> getNeighbours(long tractID){

       return tractNeighbours.getNeighbours(tractID);
    }


}
