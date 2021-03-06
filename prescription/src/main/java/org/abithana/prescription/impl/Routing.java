package org.abithana.prescription.impl;

import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.PathWrapper;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.util.PointList;

import java.io.Serializable;
import java.util.Locale;

/**
 * Created by Thilina on 1/12/2017.
 */
public class Routing implements Serializable{

    private double distance;
    private long timeInMs;

    public static GraphHopper getRoute(){
        GraphHopper hopper = new GraphHopper().forServer();

        String osmFile = "D:\\FYP\\Map\\california-latest.osm.pbf";
        String graphFolder = "D:\\FYP\\Map\\GenMaps";

        hopper.setOSMFile(osmFile);
        // where to store graphhopper files?
        hopper.setGraphHopperLocation(graphFolder);
        hopper.setEncodingManager(new EncodingManager("car"));
        // now this can take minutes if it imports or a few seconds for loading
        // of course this is dependent on the area you import
        hopper.importOrLoad();
        return hopper;
    }

    public int calc(GraphHopper hopper,Double latFrom, Double  lonFrom, Double latTo, Double lonTo){

        GHRequest req = new GHRequest(latFrom, lonFrom, latTo, lonTo).
                setWeighting("fastest").
                setVehicle("car").
                setLocale(Locale.US);
        GHResponse rsp = hopper.route(req);

        if(rsp.hasErrors()) {
            // handle them!
            // rsp.getErrors()
            System.out.println("error occuredd");
            System.out.println("lat lon from : " +latFrom +","+lonFrom);
            System.out.println("lat lon from : " +latTo +","+lonTo);


        } else {
            // use the best path, see the GHResponse class for more possibilities.
            PathWrapper path = rsp.getBest();
            // points, distance in meters and time in millis of the full path
            PointList pointList = path.getPoints();
            distance = path.getDistance();
            timeInMs = path.getTime();

           // System.out.println("distance from Montpelier to Brattleboro is = " +distance+ " mtr");


        }
        return (int)distance;

    }

}
