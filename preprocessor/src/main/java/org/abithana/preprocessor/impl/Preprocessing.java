package org.abithana.preprocessor.impl;

import org.abithana.beans.CrimeDataBeanWithTime;
import org.abithana.beans.CrimeTestBeanWithTIme;
import org.abithana.utill.Config;
import org.abithana.utill.CrimeUtil;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;
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

                        String s;
                        String year;
                        if(row.get(0).getClass()== java.sql.Timestamp.class){
                           s = ""+row.getTimestamp(0);
                            String dates[]=s.split(" ");
                            String year_mnth_day=dates[0];
                            year= year_mnth_day.substring(0,4);

                        }
                        else{
                             s = ""+row.getString(0);
                            String dates[]=s.split(" ");
                            String year_mnth_day=dates[0];
                            year= year_mnth_day.substring(year_mnth_day.length()-4,year_mnth_day.length());
                        }


                        Pattern pattern = Pattern.compile("(\\d{1,2})[:]\\d{1,2}[^:-]");
                        // Now create matcher object.
                        Matcher m = pattern.matcher(s);
                        int time = 0;

                        if (m.find()) {
                            time = Integer.parseInt(m.group(1));
                        }

                        int date=Integer.parseInt(year);
                        CrimeDataBeanWithTime crimeDataBean;

                        if(row.getString(1).equals("LARCENY/THEFT")||row.getString(1).equals("NON-CRIMINAL")||row.getString(1).equals("ASSAULT")||row.getString(1).equals("VEHICLE THEFT")||row.getString(1).equals("BURGLARY")||row.getString(1).equals("VANDALISM")||row.getString(1).equals("WARRANTS")||row.getString(1).equals("SUSPICIOUS OCC")||row.getString(1).equals("OTHER OFFENSES")){

                            crimeDataBean = new CrimeDataBeanWithTime(date,time,row.getString(1),row.getString(3),row.getString(4),row.getString(5),row.getDouble(7),row.getDouble(8));
                        }
                        else{
                            crimeDataBean = new CrimeDataBeanWithTime(date,time,"MINOR CRIMES",row.getString(3),row.getString(4),row.getString(5),row.getDouble(7),row.getDouble(8));

                        }
                         return crimeDataBean;
                    }
                });
                myDataframe = Config.getInstance().getSqlContext().createDataFrame(crimeDataBeanJavaRDD, CrimeDataBeanWithTime.class);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return myDataframe;
    }

    /*for a givn data frame it indexed the time column 1-24
    * if time column exists it convet it to 1-24 if only ate column exists convert date into time and indexed
    * */
    public DataFrame getTimeIndexedDFforTest(DataFrame df,String columnWithTime){

        DataFrame myDataframe=df;
        try{
            CrimeUtil crimeUtil=new CrimeUtil();
            boolean colexists=crimeUtil.isColExists(df,columnWithTime);

            if(colexists) {

                JavaRDD<CrimeTestBeanWithTIme> crimeTestBeanJavaRDD = df.javaRDD().map(new Function<Row, CrimeTestBeanWithTIme>() {
                    public CrimeTestBeanWithTIme call(Row row) {

                        String s;
                        String year;
                        if(row.get(0).getClass()== java.sql.Timestamp.class){
                            s = ""+row.getTimestamp(0);
                            String dates[]=s.split(" ");
                            String year_mnth_day=dates[0];
                            year= year_mnth_day.substring(0,4);

                        }
                        else{
                            s = ""+row.getString(0);
                            String dates[]=s.split(" ");
                            String year_mnth_day=dates[0];
                            year= year_mnth_day.substring(year_mnth_day.length()-4,year_mnth_day.length());
                        }


                        Pattern pattern = Pattern.compile("(\\d{1,2})[:]\\d{1,2}[^:-]");
                        // Now create matcher object.
                        Matcher m = pattern.matcher(s);
                        int time = 0;

                        if (m.find()) {
                            time = Integer.parseInt(m.group(1));
                        }

                        int date=Integer.parseInt(year);
                        CrimeTestBeanWithTIme crimeTestBean = new CrimeTestBeanWithTIme(date,time,row.getString(2),row.getString(3),row.getString(4),row.getDouble(6),row.getDouble(7));

                        return crimeTestBean;
                    }
                });

                myDataframe = Config.getInstance().getSqlContext().createDataFrame(crimeTestBeanJavaRDD, CrimeTestBeanWithTIme.class);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return myDataframe;
    }
}
