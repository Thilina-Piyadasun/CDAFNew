package org.abithana.beans;

import java.io.Serializable;

/**
 * Created by Thilina on 12/19/2016.
 */
public class TestBean implements Serializable{

    int population;
    double lat;

    public TestBean(int population, double lat) {
        this.population = population;
        this.lat = lat;
    }
}
