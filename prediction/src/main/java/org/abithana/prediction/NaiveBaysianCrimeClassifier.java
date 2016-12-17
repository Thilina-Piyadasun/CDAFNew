package org.abithana.prediction;


import org.abithana.utill.Config;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
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
 * Created by Thilina on 8/13/2016.
 */
public class NaiveBaysianCrimeClassifier extends ClassificationModel {

    public NaiveBaysianCrimeClassifier(String[] feature_columns, String label) {
        this.testFeature_columns = feature_columns;
        this.feature_columns=feature_columns;
        this.label = label;
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

        NaiveBayes nb = new NaiveBayes()
                .setLabelCol(indexedLabel)
                .setFeaturesCol(indexedFeatures);
        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol(prediction)
                .setOutputCol(predictedLabel)
                .setLabels(labelIndexer.labels());

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, nb, labelConverter});
        return pipeline;

    }


}
