package org.abithana.preprocessor.impl;

import org.abithana.beans.CrimeDataBeanWithTime;
import org.abithana.utill.Config;
import org.abithana.utill.CrimeUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by acer on 11/21/2016.
 */
public class Preprocessing implements Serializable{

    String[] fetureSet[];
    int rowLimit;

    public DataFrame dropCol(DataFrame df,String column){

        try{

            df=df.drop(column);
        }catch (Exception e){

        }
        return df;

    }
    public Column getCol(DataFrame df,String column){

        Column col=null;
        try{
            col=df.col(column);
        }catch (Exception e){

        }
        return col;

    }
    public DataFrame aggregateDataFrames(DataFrame f1,DataFrame f2){
        return null;
        
    }

    public DataFrame stringIndexing(DataFrame df){
        return df;

    }

    public DataFrame createFeatureFrame(DataFrame f1){
        return null;

    }

    public DataFrame removeDupliates(DataFrame f1){
        return null;

    }

    public String[][] getFetureSet() {
        return fetureSet;
    }

    public void setFetureSet(String[][] fetureSet) {
        this.fetureSet = fetureSet;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

    /*for a givn data frame it indexed the time column 1-24
    * if time column exists it convet it to 1-24 if only ate column exists convert date into time and indexed
    * */
    public DataFrame getTimeIndexedDF(DataFrame df,String columnWithTime){

        DataFrame myDataframe=df;
        try{
            CrimeUtil crimeUtil=new CrimeUtil();
            boolean colexists=crimeUtil.isColExists(df,columnWithTime);

            if(colexists) {

                JavaRDD<CrimeDataBeanWithTime> crimeDataBeanJavaRDD = df.javaRDD().map(new Function<Row, CrimeDataBeanWithTime>() {
                    public CrimeDataBeanWithTime call(Row row) {

                        String s = ""+row.getTimestamp(0);

                        Pattern pattern = Pattern.compile("(\\d{1,2})[:]\\d{1,2}[^:-]");
                        // Now create matcher object.
                        Matcher m = pattern.matcher(s);
                        int time = 0;

                        if (m.find()) {
                            time = Integer.parseInt(m.group(1));
                        }

                        String dates[]=s.split(" ");
                        String year_mnth_day=dates[0];
                        String year= year_mnth_day.substring(0,4);

                        int date=Integer.parseInt(year);
                        CrimeDataBeanWithTime crimeDataBean = new CrimeDataBeanWithTime(date,time,row.getString(1),row.getString(3),row.getString(4),row.getString(5),row.getDouble(7),row.getDouble(8));
                        return crimeDataBean;
                    }
                });
                myDataframe = Config.getInstance().getSqlContext().createDataFrame(crimeDataBeanJavaRDD, CrimeDataBeanWithTime.class);
                myDataframe.show(30);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return myDataframe;
    }
}
