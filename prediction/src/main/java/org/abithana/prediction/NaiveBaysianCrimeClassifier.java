package org.abithana.prediction;


import org.abithana.utill.Config;
import org.apache.spark.ml.Model;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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


    Pipeline getPipeline(DataFrame trainData){

/*
        NaiveBayes nb = new NaiveBayes().setSmoothing(0.00001);
        Tokenizer tokenizer = new Tokenizer().setInputCol(label).setOutputCol(indexedLabel);
        HashingTF hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol()).setOutputCol(predictedLabel);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, hashingTF, nb});*/

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol(label)
                .setOutputCol(indexedLabel)
                .fit(trainData);

        try {
            labelIndexer.write().overwrite().save("models\\Nivebayes\\label");
        } catch (IOException e) {
            System.out.println("====================================");
            System.out.println("CANNOT SAVE LABEL INDEXER");
            System.out.println("====================================");
            e.printStackTrace();
        }
        System.out.println("label indexer saved");
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol(generated_feature_col_name)
                .setOutputCol(indexedFeatures)
                .setMaxCategories(40)
                .fit(trainData);
        try {
            featureIndexer.write().overwrite().save("models\\Nivebayes\\vector");
        } catch (IOException e) {
            System.out.println("====================================");
            System.out.println("CANNOT SAVE FEATURE INDEXER");
            System.out.println("====================================");
            e.printStackTrace();
        }
        System.out.println("FEATURE indexer saved");

        NaiveBayes nb = new NaiveBayes()
                .setLabelCol(indexedLabel)
                .setFeaturesCol(indexedFeatures);
        try {
            featureIndexer.write().overwrite().save("models\\Nivebayes\\naivebays");
        } catch (IOException e) {
            System.out.println("====================================");
            System.out.println("CANNOT SAVE NaiveBayes ");
            System.out.println("====================================");
            e.printStackTrace();
        }
        System.out.println("NaiveBayes saved");
        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol(prediction)
                .setOutputCol(predictedLabel)
                .setLabels(labelIndexer.labels());
        try {
            featureIndexer.write().overwrite().save("models\\Nivebayes\\IndexToString");
        } catch (IOException e) {
            System.out.println("====================================");
            System.out.println("CANNOT SAVE IndexToString ");
            System.out.println("====================================");
            e.printStackTrace();
        }
        System.out.println("IndexToString saved");

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, nb, labelConverter});

        try {
            featureIndexer.write().overwrite().save("models\\Nivebayes\\Pipeline");
        } catch (IOException e) {
            System.out.println("====================================");
            System.out.println("CANNOT SAVE Pipeline ");
            System.out.println("====================================");
            e.printStackTrace();
        }
        System.out.println("Pipeline saved");
        return pipeline;

    }



}
