package org.abithana.frontConnector;

import org.abithana.ds.CrimeDataStore;
import org.abithana.ds.DataStore;
import org.abithana.ds.PopulationDataStore;
import org.abithana.ds.PreprocessedCrimeDataStore;
import org.abithana.prediction.*;
import org.abithana.preprocessor.facade.PreprocessorFacade;
import org.abithana.utill.Config;
import org.apache.spark.sql.DataFrame;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Thilina on 1/19/2017.
 */
public class Visualizer_Prediction {

    private ClassificationModel classificationModel;
    private String[] dropColumns={"resolution","descript","address"};

    PreprocessorFacade preprocessorFacade=new PreprocessorFacade();
    CrimeDataStore initaldataStore = CrimeDataStore.getInstance();
    PreprocessedCrimeDataStore preProcesedDataStore= PreprocessedCrimeDataStore.getInstance();
    PopulationDataStore populationDataStore=new PopulationDataStore();


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

    public void doPreprocessing(String prepTableName){

        DataFrame df= initaldataStore.getDataFrame();
        DataFrame f2=preprocessorFacade.handelMissingValues(df);

        List columns= Arrays.asList(f2.columns());
        if(columns.contains("dateAndTime")&&(!columns.contains("Time"))) {
            f2=preprocessorFacade.getTimeIndexedDF(f2, "dateAndTime");
        }

        for(String s: dropColumns){
            f2=preprocessorFacade.dropCol(f2,s);
        }
        f2.show(3);
        /*At final step in preprocessing save data frame in PreprocessedDataStore*/
        preProcesedDataStore.saveTable(f2, prepTableName);

        f2=preprocessorFacade.integratePopulationData(populationDataStore.getTableName(),prepTableName);
        System.out.println("+++++++++++++++++++++++++++++");
        System.out.println("==============================================");
        f2.show(3);
        preProcesedDataStore.saveTable(f2,prepTableName);
        preProcesedDataStore.getDataFrame().show(40);

    }

    /*
    * get all columns and show them to user
    * */
    public String[] getPopulationDataColumns(String path){
        String[] columns=populationDataStore.getColumns(path);
        return columns;
    }

    /*
    * format dataset in a unique way beofore integrate with user
    * */
    public void formattingPopulationDataSet(String tableName){

        String populationTableName=tableName;

        List<String> str=populationDataStore.getUserRevisedColumns();

        for(String s1:str){
            System.out.println(s1);
        }
        populationDataStore.saveTable(populationTableName);
    }

    public String[] getColumnNames(String prepTableName){
        return preProcesedDataStore.showColumns(prepTableName);
    }

    /*
    * predict for a given file using previously trained model
    * */
    public boolean predict(String testFIlePath){

        try{
            //"D:\\FYP\\test.csv"
            Config instance=Config.getInstance();
            DataFrame df=instance.getSqlContext().read()
                    .format("com.databricks.spark.csv")
                    .option("header","true")
                    .option("inferSchema","true")
                    .load(testFIlePath);


            DataFrame f2=preprocessorFacade.handelMissingValues(df);

            List columns= Arrays.asList(f2.columns());
            if(columns.contains("Dates")&&(!columns.contains("Time"))) {
                f2=preprocessorFacade.getTimeIndexedTestDF(f2, "Dates");
            }

            for(String s: dropColumns){
                f2=preprocessorFacade.dropCol(f2,s);
            }
            f2.registerTempTable("test");
            f2=preprocessorFacade.integratePopulationData(populationDataStore.getTableName(),"test");

            return classificationModel.predict(f2);

        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===================PROBLEM OCCURED WHILE PREDICTING=====================");
        return false;
    }

    /*
    * NB pipeline model
    * */
    public List<Evaluation> train_NBpipeline(String[] featureCol,String label){
        try{
            // String[] featureCol = {"dayOfWeek", "pdDistrict","time","year","month","population"};
            // String label = "category";

            classificationModel=new NaiveBaysianCrimeClassifier(featureCol,label);
            classificationModel.train_pipelineModel(preProcesedDataStore.getDataFrame(), 0.8);
            List<Evaluation> list=classificationModel.getEvaluationResult();
            printEval(list); //print in console
            return list;

        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===================PROBLEM OCCURED WHILE TRAINING NAIVE BAYES MODEL=====================");
        return  null;
    }

    /*
   * NB Crossvalidation model
   * */
    public List<Evaluation> train_NBCrossValidation(String[] featureCol,String label,int folds){
        try{
            //String[] featureCol = {"dayOfWeek", "pdDistrict","time","year","month","population"};
            //String label = "category";

            classificationModel=new NaiveBaysianCrimeClassifier(featureCol,label);
            classificationModel.train_crossValidatorModel(preProcesedDataStore.getDataFrame(), 0.8,folds);
            List<Evaluation> list=classificationModel.getEvaluationResult();

            printEval(list); //print in console
            return list;

        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===================PROBLEM OCCURED WHILE TRAINING NAIVE BAYES MODEL=====================");
        return  null;
    }

    /*
  * RF pipeline model
  * */
    public List<Evaluation> train_RFpipeline(String[] featureCol,String label,int trees,int seed){
        try{
            // String[] featureCol = {"dayOfWeek", "pdDistrict","time","year","month","population"};
            // String label = "category";

            classificationModel=new RandomForestCrimeClassifier(featureCol,label,trees,seed);
            classificationModel.train_pipelineModel(preProcesedDataStore.getDataFrame(), 0.8);
            List<Evaluation> list=classificationModel.getEvaluationResult();

            printEval(list); //print in console
            return list;

        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===================PROBLEM OCCURED WHILE TRAINING RF pipeline MODEL=====================");
        return  null;
    }


    /*
   * RF Crossvalidation model
   * */
    public List<Evaluation> train_RFCrossValidation(String[] featureCol,String label,int trees,int seed,int folds){
        try{
            //String[] featureCol = {"dayOfWeek", "pdDistrict","time","year","month","population"};
            //String label = "category";

            classificationModel=new RandomForestCrimeClassifier(featureCol,label,trees,seed);
            classificationModel.train_crossValidatorModel(preProcesedDataStore.getDataFrame(), 0.8,folds);
            List<Evaluation> list=classificationModel.getEvaluationResult();

            printEval(list); //for printing
            return list;

        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===================PROBLEM OCCURED WHILE TRAINING RF Crossvalidation MODEL=====================");
        return  null;
    }

    /*
* MLP pipeline model
* */
    public List<Evaluation> train_MLPpipeline(String[] featureCol,String label,int[] layers,int blockSize,long seed){
        try{
            // String[] featureCol = {"dayOfWeek", "pdDistrict","time","year","month","population"};
            // String label = "category";

            classificationModel=new MultilayerPerceptronCrimeClassifier(featureCol,label,layers,blockSize,seed);
            classificationModel.train_pipelineModel(preProcesedDataStore.getDataFrame(), 0.8);
            List<Evaluation> list=classificationModel.getEvaluationResult();
            printEval(list); //print in console
            return list;

        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===================PROBLEM OCCURED WHILE TRAINING MLPpipeline MODEL=====================");
        return  null;
    }


    /*
   * MLP Crossvalidation model
   * */
    public List<Evaluation> train_MLPCrossValidation(String[] featureCol,String label,int[] layers,int blockSize,long seed,int folds){
        try{
            //String[] featureCol = {"dayOfWeek", "pdDistrict","time","year","month","population"};
            //String label = "category";

            classificationModel=new MultilayerPerceptronCrimeClassifier(featureCol,label,layers,blockSize,seed);
            classificationModel.train_crossValidatorModel(preProcesedDataStore.getDataFrame(), 0.8,folds);
            List<Evaluation> list=classificationModel.getEvaluationResult();
            printEval(list); //for printing
            return list;

        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===================PROBLEM OCCURED WHILE TRAINING MLP Crossvalidation MODEL=====================");
        return  null;
    }

    /*Returns preprocess data table name*/
    public String getPreprocessTableName(){
        return preProcesedDataStore.getTableName();
    }
    public String getLongitudeCol() {
        return populationDataStore.getLongitudeCol();
    }

    public void setLongitudeCol(String longitudeCol) {
        populationDataStore.setLongitudeCol(longitudeCol);
    }

    public String getLatitudeCol() {
        return populationDataStore.getLatitudeCol();
    }

    public void setLatitudeCol(String latitudeCol) {
        populationDataStore.setLatitudeCol(latitudeCol);
    }

    public String getPopulationCol() {
        return populationDataStore.getPopulationCol();
    }

    public void setPopulationCol(String populationCol) {
        populationDataStore.setPopulationCol(populationCol);
    }

    public void printEval(List<Evaluation> list){
        for(Evaluation e:list){
            System.out.println(e.getCategory() +" precision -"+ e.getPrecision()+ " recall - "+e.getRecall()+" fmeasure-"+e.getFmeasure());
        }
    }
}
