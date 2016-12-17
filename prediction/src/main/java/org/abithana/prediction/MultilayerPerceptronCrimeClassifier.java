package org.abithana.prediction;


import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.DataFrame;

/**
 * Created by Thilina on 8/13/2016.
 */
public class MultilayerPerceptronCrimeClassifier extends ClassificationModel {

    int[] layers;
    int blockSize;
    long seed;
    int maxIterations;

    public MultilayerPerceptronCrimeClassifier(String[] feature_columns, String label,int[] layers,int blockSize,long seed,int maxIterations) {
        this.testFeature_columns = feature_columns;
        this.feature_columns=feature_columns;
        this.label = label;
        this.layers=layers;
        this.blockSize=blockSize;
        this.maxIterations=maxIterations;
        this.seed=seed;
    }


    Pipeline getPipeline(DataFrame trainData, DataFrame testData){

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol(label)
                .setOutputCol(indexedLabel)
                .fit(trainData);

        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol(generated_feature_col_name)
                .setOutputCol(indexedFeatures)
                .setMaxCategories(40)
                .fit(trainData);

        VectorIndexerModel featureIndexerTest = new VectorIndexer()
                .setInputCol(generated_feature_col_name)
                .setOutputCol(indexedFeatures)
                .setMaxCategories(40)
                .fit(testData);

        MultilayerPerceptronClassifier rf = new MultilayerPerceptronClassifier()
                .setPredictionCol(prediction)
                .setLayers(layers)
                .setBlockSize(blockSize)
                .setSeed(seed)
                .setMaxIter(maxIterations)
                .setLabelCol(indexedLabel)
                .setFeaturesCol(indexedFeatures)
                .setSeed(1100);

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol(prediction)
                .setOutputCol(predictedLabel)
                .setLabels(labelIndexer.labels());

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});
        return pipeline;

    }

}
