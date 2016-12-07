package org.abithana.prediction;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.*;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;


import java.util.HashMap;
import java.util.Vector;

/**
 * Created by Thilina on 8/13/2016.
 */
public class RandomForestCrimeClassifierImpl implements ClassificationModel {


    String generated_feature_col_name="features";
    String indexedLabel="indexedLabel";
    String indexedFeatures="indexedFeatures";
    String prediction ="prediction";
    String predictedLabel="predictedLabel";

    private String[] feature_columns;
    private String label;

    public RandomForestCrimeClassifierImpl( String[] feature_columns, String label) {
        this.feature_columns = feature_columns;
        this.label = label;

    }

    public DataFrame train_and_Predict(DataFrame trainData,DataFrame testData)throws Exception{

        if(trainData==null){
          //  trainData.setTrainDF(preproceessInitialFrame());
        }

        DataFrame featuredDF=getFeaturesFrame(trainData, feature_columns);
        DataFrame featuredTestData=getFeaturesFrame(testData, feature_columns);

        new VectorIndexer()
                .setInputCol(generated_feature_col_name)
                .setOutputCol(indexedFeatures)
                .setMaxCategories(25)
                .fit(featuredTestData);


        PipelineModel model=train(featuredDF,label,predictedLabel);
        // Make predictions.
        DataFrame predictions = model.transform(featuredTestData);
        predictions.show(10);
        // Select example row-s to display.
        predictions.select(predictedLabel, "features").show(50);
        return predictions;
    }


    /*
    * Use only train set to train model and get accuracy
    * */
    public MulticlassMetrics evaluateModel(DataFrame dataFrame,double partition)throws Exception{

        DataFrame output=getFeaturesFrame(dataFrame, feature_columns);

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol(label)
                .setOutputCol(indexedLabel)
                .fit(output);

        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol(generated_feature_col_name)
                .setOutputCol(indexedFeatures)
                .setMaxCategories(4)
                .fit(output);

        if(partition>=1){
           partition=0.7;
        }
        DataFrame[] splits = output.randomSplit(new double[] {partition,1-partition});
        DataFrame trainingData = splits[0];
        DataFrame testData = splits[1];

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol(indexedLabel)
                .setFeaturesCol(indexedFeatures);

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol(prediction)
                .setOutputCol(predictedLabel)
                .setLabels(labelIndexer.labels());

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

        PipelineModel model = pipeline.fit(trainingData);



            if(model!=null){
                DataFrame predictions = model.transform(testData);

                predictions.show(100);

                DataFrame predictionAndLabels = predictions.select("prediction", "indexedLabel");
                MulticlassMetrics metrics= new MulticlassMetrics(predictionAndLabels) ;

                Matrix confusion=metrics.confusionMatrix();
                System.out.println("confusion metrix : \n" + confusion);
                System.out.println("Accuracy = " + metrics.precision());

                System.out.println("Precision of OTHER OFFENSES = " + metrics.precision(0));
                System.out.println("Recall of OTHER OFFENSES = " + metrics.recall(0));

                System.out.println("Precision of LARCENY/THEFT = " + metrics.precision(1));
                System.out.println("Recall of LARCENY/THEFT = " + metrics.recall(1));

                System.out.println("Precision of NON-CRIMINAL = " + metrics.precision(2));
                System.out.println("Recall of NON-CRIMINAL = " + metrics.recall(2));

                System.out.println("Precision of LARCENY/THEFT = " + metrics.precision(3));
                System.out.println("Recall of LARCENY/THEFT = " + metrics.recall(3));

                System.out.println("Precision of DRUG/NARCOTIC = " + metrics.precision(4));
                System.out.println("Recall of DRUG/NARCOTIC = " + metrics.recall(4));

                System.out.println("Precision of VEHICLE THEFT = " + metrics.precision(5));
                System.out.println("Recall of VEHICLE THEFT = " + metrics.recall(5));

                System.out.println("Precision of VANDALISM = " + metrics.precision(6));
                System.out.println("Recall of VANDALISM = " + metrics.recall(6));

                System.out.println("Precision of WARRANTS = " + metrics.precision(7));
                System.out.println("Recall of WARRANTS = " + metrics.recall(7));

                System.out.println("Precision of BURGLARY = " + metrics.precision(8));
                System.out.println("Recall of BURGLARY = " + metrics.recall(8));

                System.out.println("Precision of SUSPICIOUS OCC = " + metrics.precision(9));
                System.out.println("Recall of SUSPICIOUS OCC = " + metrics.recall(9));

                return metrics;
           /* MulticlassMetrics metrics = new MulticlassMetrics(predictions);

            Matrix confusion = metrics.confusionMatrix();

            System.out.println("Confusion matrix: \n" + confusion);
            System.out.println("Accuracy = " + metrics.precision());

            *//*MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                    .setLabelCol("indexedLabel")
                    .setPredictionCol("prediction")
                    .setMetricName("precision");

            double accuracy = evaluator.evaluate(predictions);

            System.out.println("Test Error = " + (1.0 - accuracy));

            RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);
            System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());*//*
            return metrics.precision();*/


        }
        else {
            throw new Exception("no trained randomForest classifier model found");
        }
    }

    /*
  *   featuredDF - vector assemblered data frame to train the model
  *   label - label column in  train data set
  *   predictedLabel -a new column name to generate predictions to test data set and those predictions store under that column name
  *
  *   this method trains Random forest classifier model and return the model
  * */
    private PipelineModel train(DataFrame featuredDF,String label, String predictedLabel) {

        try{
            StringIndexerModel labelIndexer = new StringIndexer()
                    .setInputCol(label)
                    .setOutputCol(indexedLabel)
                    .fit(featuredDF);

            // Automatically identify categorical features, and index them.
            // Set maxCategories so features with > 4 distinct values are treated as continuous.
            VectorIndexerModel featureIndexer = new VectorIndexer()
                    .setInputCol("features")
                    .setOutputCol(indexedFeatures)
                    .setMaxCategories(25)
                    .fit(featuredDF);

            RandomForestClassifier rf = new RandomForestClassifier()
                    .setLabelCol(indexedLabel)
                    .setFeaturesCol(indexedFeatures);

            // Convert indexed labels back to original labels.
            IndexToString labelConverter = new IndexToString()
                    .setInputCol(prediction)
                    .setOutputCol(predictedLabel)
                    .setLabels(labelIndexer.labels());

            Pipeline pipeline = new Pipeline()
                    .setStages(new PipelineStage[]{labelIndexer, featureIndexer, rf, labelConverter});

            PipelineModel model = pipeline.fit(featuredDF);
            return model;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /*
    * generate extra feature vector column to given dataset
    * */
    public DataFrame getFeaturesFrame(DataFrame df,String[] featureCols){

        return new MLDataParser().getFeaturesFrame(df,featureCols, generated_feature_col_name);
    }

    public DataFrame predict(Vector<Object> features) {
        return null;
    }

    public Vector evaluate() {
        return null;
    }

   /* public Model setGISData(HashMap<String, Vector> dataSet) {

        return null;
    }

    public Model setWeatherData(HashMap<String, Vector> dataSet) {
        return null;
    }
*/

}
