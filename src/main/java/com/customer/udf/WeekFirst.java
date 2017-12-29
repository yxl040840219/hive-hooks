package com.customer.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by yxl on 2017/12/29.
 */
public class WeekFirst extends UDF {

    public String evaluate(final String str)  {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd") ;
        Date date = null ;
        try {
            date = sdf.parse(str) ;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return weekFirst(date) ;

    }

    private String weekFirst(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd") ;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        if (c.get(Calendar.DAY_OF_WEEK) == 1) {
            c.add(Calendar.DAY_OF_MONTH, -1);
        }
        c.add(Calendar.DATE, c.getFirstDayOfWeek() - c.get(Calendar.DAY_OF_WEEK) + 1);
        return sdf.format(c.getTime());
    }

    public String evaluate(final Date date)  {
        return weekFirst(date) ;
    }

}
