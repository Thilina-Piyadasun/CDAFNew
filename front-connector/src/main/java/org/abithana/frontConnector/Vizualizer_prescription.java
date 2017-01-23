package org.abithana.frontConnector;

import org.abithana.ds.CrimeDataStore;
import org.abithana.ds.PreprocessedCrimeDataStore;
import org.abithana.preprocessor.facade.PreprocessorFacade;
import org.abithana.prescription.impl.PatrolBoundry;
import org.abithana.prescription.impl.PrescriptionData;
import org.abithana.prescription.impl.TractCentroid;
import org.apache.spark.sql.DataFrame;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Thilina on 1/22/2017.
 */
public class Vizualizer_prescription {

    PreprocessorFacade preprocessorFacade=new PreprocessorFacade();
    PreprocessedCrimeDataStore preprocessedCrimeDataStore=PreprocessedCrimeDataStore.getInstance();
    CrimeDataStore initaldataStore=CrimeDataStore.getInstance();
    PrescriptionData prescriptionData=new PrescriptionData();
    TractCentroid t=new TractCentroid();

    public void readFile(String path,String tableName){

        initaldataStore.getColumns(path);
        initaldataStore.setDatesCol("Dates");
        initaldataStore.setCategoryCol("Category");
        initaldataStore.setDayOfWeekCol("DayOfWeek");
        initaldataStore.setPdDistrictCol("PdDistrict");
        initaldataStore.setResolution("Resolution");
        initaldataStore.setLatitudeCol("Y");
        initaldataStore.setLongitudeCol("X");
        initaldataStore.saveTable(tableName).show(30);
    }

    /*
    this method runs if there is no preprocessing done previously
    */
    private void doPreprocessing(){
        /*get initial dataFrame and preprocess it only for prescription*/
        DataFrame df=preprocessorFacade.handelMissingValues(initaldataStore.getDataFrame());
        DataFrame f2=preprocessorFacade.handelMissingValues(df);

        List columns= Arrays.asList(f2.columns());
        if(columns.contains("dateAndTime")&&(!columns.contains("Time"))) {
            f2=preprocessorFacade.getTimeIndexedDF(f2, "dateAndTime");
        }
        System.out.println("==================================================================================");
        System.out.println("                            PREPROCESSED DATA");
        System.out.println("==================================================================================");
        f2.show(30);
        preprocessedCrimeDataStore.saveTable(f2, "preprocessedData");
    }

    public Map<Integer,List> generatePatrolBeats(){

        if(preprocessedCrimeDataStore.getDataFrame()==null) {
            doPreprocessing();
        }

        String tblname=preprocessedCrimeDataStore.getTableName();
        prescriptionData.setCategory("category");
        prescriptionData.setLat("latitude");
        prescriptionData.setLon("longitude");
        String query=prescriptionData.patrolQueryGenerator(tblname, 0, 3);
        String prescriptionTblName="prescription";
        prescriptionData.createPrescriptionDs(prescriptionTblName,query);

       // prescriptionData.digitizeMap(tblname,prescriptionTblName);

        PatrolBoundry p=new PatrolBoundry();

        p.getLearders( t.getAllTractCentroids(prescriptionTblName),10);
        p.calcThreashold( t.getToalWork(),10);
        p.findPatrolBoundries();
        return p.getBoundryTractids();
    }





}
