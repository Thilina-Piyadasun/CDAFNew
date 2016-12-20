package org.abithana.frontConnector;

import org.abithana.ds.CrimeDataStore;
import org.abithana.ds.DataStore;
import org.abithana.ds.PreprocessedCrimeDataStore;
import org.abithana.prediction.*;
import org.abithana.preprocessor.facade.PreprocessorFacade;
import org.abithana.stat.facade.StatFacade;
import org.abithana.statBeans.CordinateBean;
import org.abithana.statBeans.HistogramBean;
import org.abithana.utill.Config;
import org.abithana.utill.Converter;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

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
    private ClassificationModel classificationModel;

    private String[] dropColumns={"resolution","descript","address"};

    PreprocessorFacade preprocessorFacade=new PreprocessorFacade();
    DataStore initaldataStore =CrimeDataStore.getInstance();
    DataStore preProcesedDataStore= PreprocessedCrimeDataStore.getInstance();
    Converter converter=new Converter();

    public void readFile(String path,String tblName){
        this.path=path;
        this.tblName=tblName;
        initaldataStore.read_file(path, tblName);
    }

    public void doPreprocessing(String prepTableName){

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
        preProcesedDataStore.saveTable(f2, prepTableName);
        preProcesedDataStore.getDataFrame().show(40);

    }

    public String[] getColumnNames(String prepTableName){
        return preProcesedDataStore.showColumns(prepTableName);
    }

    public void predict(){

        String[] featureCol = {"dayOfWeek", "pdDistrict","time","year"};
        String label = "category";
        int[] layers = new int[]{featureCol.length,500,39};
        RandomForestCrimeClassifier rf=new RandomForestCrimeClassifier(featureCol,label,20, 1234);
        try{
            Config instance=Config.getInstance();
            DataFrame df=instance.getSqlContext().read()
                    .format("com.databricks.spark.csv")
                    .option("header","true")
                    .option("inferSchema","true")
                    .load("D:\\FYP\\test.csv");
            DataFrame df1=rf.train_pipelineModel(preProcesedDataStore.getDataFrame(), df, 0.8);
            df1.show(30);
            List<Evaluation> list=rf.getEvaluationResult();
            for(Evaluation e:list){
                System.out.println(e.getCategory() +" precision -"+ e.getPrecision()+ " recall - "+e.getRecall()+" fmeasure-"+e.getFmeasure());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /*Returns preprocess data table name*/
    public String getPreprocessTableName(){
        return preProcesedDataStore.getTableName();
    }
    /*
    * to execute queries from visualization
    * */
    public ArrayList<ArrayList> executeQueries(String query){
        try {
            DataFrame dataFrame= initaldataStore.queryDataSet(query);
            ArrayList<ArrayList> list=converter.convert(dataFrame);
            return list;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /*
    for a given category retruns the freuquency of respective crime category in each year
    eg:
    2001 34
    2002 505
    2003 56
    * */
    public List<HistogramBean> categoryWiseData(String category){
       DataFrame df= preProcesedDataStore.getDataFrame();
        StatFacade statFacade=new StatFacade();
        DataFrame dataFrame=statFacade.categoryTimeData(df, category);
        return statFacade.getVisualizeList(dataFrame);

    }

    /*
    * Data for heat ap visualization
    * */
    public List<CordinateBean> heatMapData(String[] categories){
        DataFrame df= preProcesedDataStore.getDataFrame();
        StatFacade statFacade=new StatFacade();
        DataFrame dataFrame=statFacade.categoryWiseCoordinates(df,categories);
        return statFacade.getCordinateList(dataFrame);
    }

    /*
    * for a given year freaquncy of each caegory
    * */
    public List<HistogramBean> yearWiseData(int year){
        DataFrame df= preProcesedDataStore.getDataFrame();
        StatFacade statFacade=new StatFacade();
        DataFrame dataFrame=statFacade.yearCategoryData(df, year);
        return statFacade.getVisualizeList(dataFrame);

    }

    /*
    * Data for time line animation
    * */
    public List<CordinateBean> timeLineAnimation(int startYear,int endYear){
        DataFrame df= preProcesedDataStore.getDataFrame();
        StatFacade statFacade=new StatFacade();
        DataFrame dataFrame=statFacade.categoryFrequency_givenTimeRange(df,startYear,endYear);
        return statFacade.getCordinateList(dataFrame);

    }

    /*
    * Categories of data set after preprocessing
    * */
    public List<String> getCategoories(String prepTableName){

        try {
            DataFrame dataFrame= preProcesedDataStore.queryDataSet("Select distinct category from "+prepTableName);
            Converter converter=new Converter();
            List<Row> list=dataFrame.collectAsList();
            List<String> stringList=new ArrayList<>();
            for(Row row :list){
                stringList.add(row.toString());
            }
            return stringList;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    /*
    * Prediction Methods
    * */
    public ArrayList<ArrayList> perceptronCrossVaidationModel(String testPath,String[] feature_columns, String label,int[] layers,int blockSize,long seed,int maxIterations,double partition,int folds){
        try {
            classificationModel=new MultilayerPerceptronCrimeClassifier(feature_columns,label,layers,blockSize,seed,maxIterations);
            DataFrame df=classificationModel.train_crossValidatorModel(preProcesedDataStore.getDataFrame(), initaldataStore.readCsv(testPath), partition, folds);

            ArrayList<ArrayList> list=converter.convert(df);
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<ArrayList> RForestCrossVaidationModel(String testPath,String[] feature_columns, String label,int trees,int seed,double partition,int folds){
        try {
            classificationModel=new RandomForestCrimeClassifier(feature_columns,label,trees,seed);
            DataFrame df=classificationModel.train_crossValidatorModel(preProcesedDataStore.getDataFrame(), initaldataStore.readCsv(testPath), partition,folds);

            ArrayList<ArrayList> list=converter.convert(df);
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<ArrayList> NBCrossVaidationModel(String testPath,String[] feature_columns, String label,double partition,int folds){
        try {
            classificationModel=new NaiveBaysianCrimeClassifier(feature_columns,label);
            DataFrame df=classificationModel.train_crossValidatorModel(preProcesedDataStore.getDataFrame(), initaldataStore.readCsv(testPath), partition,folds);

            ArrayList<ArrayList> list=converter.convert(df);
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<ArrayList> perceptronPipelineModel(String testPath,String[] feature_columns, String label,int[] layers,int blockSize,long seed,int maxIterations,double partition){
        try {
            classificationModel=new MultilayerPerceptronCrimeClassifier(feature_columns,label,layers,blockSize,seed,maxIterations);
            DataFrame df=classificationModel.train_pipelineModel(preProcesedDataStore.getDataFrame(), initaldataStore.readCsv(testPath), partition);

            ArrayList<ArrayList> list=converter.convert(df);
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public ArrayList<ArrayList> RForestPipelineModel(String testPath,String[] feature_columns, String label,int trees,int seed,double partition){
        try {
            classificationModel=new RandomForestCrimeClassifier(feature_columns,label,trees,seed);
            DataFrame df=classificationModel.train_pipelineModel(preProcesedDataStore.getDataFrame(), initaldataStore.readCsv(testPath), partition);

            ArrayList<ArrayList> list=converter.convert(df);
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<ArrayList> NBPipelineModel(String testPath,String[] feature_columns, String label,double partition){
        try {
            classificationModel=new NaiveBaysianCrimeClassifier(feature_columns,label);
            DataFrame df=classificationModel.train_pipelineModel(preProcesedDataStore.getDataFrame(), initaldataStore.readCsv(testPath), partition);

            ArrayList<ArrayList> list=converter.convert(df);
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public void setDropColumns(String[] dropColumns) {
        this.dropColumns = dropColumns;
    }
}
