package org.abithana.ds;

import org.abithana.beans.PopulationBean;
import org.abithana.utill.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;

/**
 * Created by Thilina on 12/19/2016.
 */
public class PopulationDataStore implements Serializable {

    private static DataFrame populationDF;
    private String populationtableName= "populationTbl";
    Config config=Config.getInstance();

    public DataFrame readFile(String filename,String tableName){

        // Load the input data to a static Data Frame
        JavaRDD<String> textFile = config.getSc().textFile(filename);
        JavaRDD<PopulationBean> rowRDD = textFile.map(
                new Function<String, PopulationBean>() {
                    public PopulationBean call(String line) throws Exception {
                        String[] cols=line.split(",");
                        PopulationBean blockData=new PopulationBean(0,0,0);
                        if(cols[0].matches("[-+]?\\d*\\.?\\d+")){
                            blockData=new PopulationBean(Integer.parseInt(cols[4]),Double.parseDouble(cols[5]),Double.parseDouble(cols[6]));

                        }
                        return blockData;
                    }
                });

        populationDF=config.getSqlContext().createDataFrame(rowRDD,PopulationBean.class);
        populationDF.show(50);

        populationDF.registerTempTable(tableName);
        cache_data(1);
        return  populationDF;
    }

    private void cache_data(int storage_level){

        if(storage_level==1)
            populationDF.persist(StorageLevel.MEMORY_ONLY());
        else if(storage_level==2)
            populationDF.persist(StorageLevel.MEMORY_ONLY_SER());
        else if(storage_level==3)
            populationDF.persist(StorageLevel.MEMORY_AND_DISK());
        else
            populationDF.persist(StorageLevel.MEMORY_AND_DISK_SER());
    }

    public DataFrame getDataFrame(){
        return populationDF;
    }

    public String getTableName() {
        return populationtableName;
    }

    public String[] showColumns(String tableName){
        if(tableName==populationtableName)
            return populationDF.columns();
        else{
            return null;
        }
    }
}
