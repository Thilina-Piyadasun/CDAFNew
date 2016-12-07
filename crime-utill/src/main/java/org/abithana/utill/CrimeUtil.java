package org.abithana.utill;

import org.abithana.beans.CrimeDataBeanWithTime;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by Thilina on 8/27/2016.
 */
public class CrimeUtil implements Serializable {

    Config instance=Config.getInstance();

    public boolean isColExists(DataFrame df, String colName)throws Exception{

        String[] columns=df.columns();

        int count=0; //for check whether  duplicate columns exists or not
        boolean colExists=false;
       /* for(String s:df.columns()){
            if(s==colName)
                return true;
        }*/
        for(int i=0;i<columns.length;i++){
            if(colName.equals(columns[i])){
                colExists=true;
                count++;
            }
        }
        if(colExists){
            if(count==1)
                return true;
            else
                throw new Exception("Duplicate column names found " + colName);
        }
        else
            throw new Exception("column not found " + colName);

    }

    /*for a givn data frame it indexed the time column 1-24
    * if time column exists it convet it to 1-24 if only ate column exists convert date into time and indexed
    * */
    /*public DataFrame getTimeIndexedDF(DataFrame df,String columnWithTime){

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

                        CrimeDataBeanWithTime crimeDataBean = new CrimeDataBeanWithTime(time,row.getString(1),row.getString(3),row.getString(4),row.getString(5),row.getDouble(7),row.getDouble(8));
                        return crimeDataBean;
                    }
                });
                myDataframe = instance.getSqlContext().createDataFrame(crimeDataBeanJavaRDD, CrimeDataBeanWithTime.class);
                myDataframe.show(30);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return myDataframe;
    }*/

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

