package org.abithana.prediction;

import org.abithana.utill.Config;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Thilina on 12/1/2016.
 */
public abstract class ClassificationModel {

    String generated_feature_col_name="features";
    String indexedLabel="indexedLabel";
    String indexedFeatures="indexedFeatures";
    String prediction ="prediction";
    String predictedLabel="predictedLabel";
    Config config=Config.getInstance();
    String[] feature_columns;
    String[] testFeature_columns;
    String label;
    List<Evaluation> evalList=new ArrayList<>();

    MLDataParser mlDataParser=new MLDataParser();


    abstract Pipeline getPipeline(DataFrame trainData, DataFrame testData);

    /*Method to get eveluation results of created model*/
    public List<Evaluation> getEvaluationResult(){
        return evalList;
    }

    /*
    * Use only getPipeline set to getPipeline model and get accuracy
    * */
    public DataFrame train_crossValidatorModel(DataFrame train, DataFrame test, double partition,int folds)throws Exception{

        /*transform trian set to features*/
        DataFrame trainData=getFeaturesFrame(train, feature_columns);

        /*preprocess user given test data set and transform it*/
        test=mlDataParser.preprocessTestData(test);
        DataFrame testData=getFeaturesFrame(test,mlDataParser.removeIndexWord(testFeature_columns));

        Pipeline pipeline= getPipeline(trainData, testData);
        if(partition>=1){
            partition=0.7;
        }

        DataFrame[] splits = trainData.randomSplit(new double[] {partition,(1-partition)});
        DataFrame trainingData = splits[0];
        DataFrame evalData = splits[1];

        ParamMap[] paramGrid = new ParamGridBuilder().build();

        // Run cross-validation, and choose the best set of parameters.
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol(indexedLabel)
                .setPredictionCol(prediction)
                        // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
                .setMetricName("precision");

        CrossValidator cv = new CrossValidator()
                // ml.Pipeline with ml.classification.RandomForestClassifier
                .setEstimator(pipeline)
                        // ml.evaluation.MulticlassClassificationEvaluator
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(folds);

        CrossValidatorModel model = cv.fit(trainingData);

        if(model!=null){
            DataFrame predictions = model.transform(testData);
            DataFrame evaluations = model.transform(evalData);
            evaluationProcess(evaluations);
            return predictions;
        }
        else {
            throw new Exception("no trained randomForest classifier model found in cross validation");
        }
    }

    public DataFrame train_pipelineModel(DataFrame train, DataFrame test, double partition)throws Exception{

        /*transform trian set to features*/
        DataFrame trainData=getFeaturesFrame(train, feature_columns);
        /*preprocess user given test data set and transform it*/
        DataFrame testData=getTestFeatureFrame(test);

        Pipeline pipeline= getPipeline(trainData, testData);

        if(partition>=1){
            partition=0.7;
        }

        DataFrame[] splits = trainData.randomSplit(new double[] {partition,(1-partition)});
        DataFrame trainingData = splits[0];
        DataFrame evalData = splits[1];

        PipelineModel model = pipeline.fit(trainingData);

        if(model!=null){
            DataFrame predictions = model.transform(testData);
            DataFrame evaluations = model.transform(evalData);
            evaluationProcess(evaluations);
            return predictions;
        }
        else {
            throw new Exception("no trained randomForest classifier model found in Pipeline Model");
        }
    }


    private void evaluationProcess(DataFrame evaluations){

        try {
            evaluations.registerTempTable("predictions");

            DataFrame predictionAndLabels = evaluations.select("prediction", "indexedLabel");
            MulticlassMetrics metrics= new MulticlassMetrics(predictionAndLabels) ;

            Matrix confusion=metrics.confusionMatrix();

            DataFrame  evaluation= config.getSqlContext().sql("select distinct category, avg(indexedLabel) as index  from predictions group by category");
            List<Row> lli=evaluation.collectAsList();

            for(Row r:lli){
                String cat=r.getString(0);
                double index=r.getDouble(1);
                System.out.println(r.getString(0)+"-"+r.getDouble(1));
                for(double i:metrics.labels()){

                    if(i==index){
                        Evaluation e=new Evaluation(i,cat,metrics.precision(i),metrics.recall(i),metrics.fMeasure(i));
                        evalList.add(e);
                        break;
                    }
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /*
     * generate extra feature vector column to given dataset
     * */
    public DataFrame getFeaturesFrame(DataFrame df,String[] featureCols){

        if(generated_feature_col_name!=null){
            return new MLDataParser().getFeaturesFrame(df,featureCols, generated_feature_col_name);
        }
        else
            return null;
    }

    public DataFrame getTestFeatureFrame(DataFrame test){
        try{
            test=mlDataParser.preprocessTestData(test);
            DataFrame testData=getFeaturesFrame(test,mlDataParser.removeIndexWord(testFeature_columns));
            return testData;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
