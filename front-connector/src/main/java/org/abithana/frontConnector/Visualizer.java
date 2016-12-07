package org.abithana.frontConnector;

import org.abithana.ds.CrimeDataStore;
import org.abithana.ds.DataStore;
import org.abithana.ds.PreprocessedCrimeDataStore;
import org.abithana.prediction.RandomForestCrimeClassifierImpl;
import org.abithana.preprocessor.facade.PreprocessorFacade;
import org.abithana.stat.facade.StatFacade;
import org.abithana.statBeans.CordinateBean;
import org.abithana.statBeans.HistogramBean;
import org.abithana.utill.Converter;
import org.apache.spark.sql.DataFrame;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by acer on 11/23/2016.
 */
public class Visualizer  implements Serializable{

    private String path;
    private String tblName;

    private String[] dropColumns={"resolution","descript","address"};

    PreprocessorFacade preprocessorFacade=new PreprocessorFacade();
    DataStore initaldataStore =CrimeDataStore.getInstance();
    DataStore preProcesedDataStore= PreprocessedCrimeDataStore.getInstance();
    public void readFile(String path,String tblName){
        this.path=path;
        this.tblName=tblName;
        initaldataStore.read_file(path, tblName);
    }
    public void doPreprocessing(String prepTaleName){

        DataFrame df= initaldataStore.getDataFrame();
        DataFrame f2=preprocessorFacade.handelMissingValues(df);

        List columns= Arrays.asList(f2.columns());
        if(columns.contains("Dates")&&(!columns.contains("Time"))) {
            f2=preprocessorFacade.getTimeIndexedDF(f2, "Dates");
        }

        for(String s: dropColumns){
            f2=preprocessorFacade.dropCol(f2,s);
        }

        /*At final step in preprocessing save data frame in PreprocessedDataStore*/
        preProcesedDataStore.saveTable(f2, prepTaleName);
        preProcesedDataStore.getDataFrame().show(40);

    }

    public void predict(){

        String[] featureCol = {"dayOfWeek", "pdDistrict","time", "x", "y"};
        String label = "category";
        RandomForestCrimeClassifierImpl rf=new RandomForestCrimeClassifierImpl(featureCol,label);
        try{
            rf.evaluateModel(preProcesedDataStore.getDataFrame(),0.8);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public ArrayList<ArrayList> executeQueries(String query){
        try {
            DataFrame dataFrame= initaldataStore.queryDataSet(query);
            Converter converter=new Converter();
            /*for(ArrayList list:converter.convert(dataFrame)){
                System.out.println(list.toString());
            }*/
            ArrayList<ArrayList> list=converter.convert(dataFrame);
            return list;

        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public List<HistogramBean> categoryWiseData(String category){
       DataFrame df= initaldataStore.getPreprocessedData();
        StatFacade statFacade=new StatFacade();
        DataFrame dataFrame=statFacade.categoryTimeData(df, category);
        return statFacade.getVisualizeList(dataFrame);

    }
    public List<CordinateBean> heatMapData(String[] categories){
        DataFrame df= initaldataStore.getPreprocessedData();
        StatFacade statFacade=new StatFacade();
        DataFrame dataFrame=statFacade.categoryWiseCoordinates(df,categories);
        dataFrame.show(20);
        return statFacade.getCordinateList(dataFrame);
    }
    public List<HistogramBean> yearWiseData(int year){
        DataFrame df= initaldataStore.getPreprocessedData();
        StatFacade statFacade=new StatFacade();
        DataFrame dataFrame=statFacade.yearCategoryData(df, year);
        return statFacade.getVisualizeList(dataFrame);

    }

    public void setDropColumns(String[] dropColumns) {
        this.dropColumns = dropColumns;
    }
}
