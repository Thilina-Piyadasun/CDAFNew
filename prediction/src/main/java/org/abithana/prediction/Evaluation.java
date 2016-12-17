package org.abithana.prediction;

import java.io.Serializable;

/**
 * Created by Thilina on 12/13/2016.
 */
public class Evaluation implements Serializable{
    double indexedLabel;
    String category;
    double Precision;
    double Recall;
    double fmeasure;

    public Evaluation(double indexedLabel,String category, double precision, double recall, double fmeasure) {
        this.indexedLabel = indexedLabel;
        this.category=category;
        Precision = precision;
        Recall = recall;
        this.fmeasure = fmeasure;
    }
}
