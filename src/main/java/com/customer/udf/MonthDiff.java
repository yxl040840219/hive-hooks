package com.customer.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.hadoop.hive.ql.exec.UDF;

public class MonthDiff extends UDF {

    public Long evaluate(final Object start, final Object end)  {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM") ;
        Calendar startCalendar = Calendar.getInstance() ;
        Calendar endCalendar = Calendar.getInstance();
        try {
            startCalendar.setTime(sdf.parse(start.toString().trim())) ;
            endCalendar.setTime(sdf.parse(end.toString().trim()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long month =(endCalendar.getTimeInMillis() - startCalendar.getTimeInMillis()) / (3600 * 1000 * 24 * 30l);
        return month ;
    }


}
