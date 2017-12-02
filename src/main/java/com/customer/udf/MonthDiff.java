package com.customer.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

class MonthDiff extends UDF {

    public long evaluate(final String start, final String end) throws HiveException {
        assert StringUtils.isNotEmpty(start) ;
        assert StringUtils.isNotEmpty(end) ;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM") ;
        Calendar startCalendar = Calendar.getInstance() ;
        Calendar endCalendar = Calendar.getInstance();
        try {
            startCalendar.setTime(sdf.parse(start.toString().trim())) ;
            endCalendar.setTime(sdf.parse(end.toString().trim()));
        } catch (ParseException e) {
            throw new HiveException("日期解析错误 yyyy-MM") ;
        }
        return monthDiff(startCalendar,endCalendar) ;
    }

    public long evaluate(final Date start, final Date end){
        Calendar startCalendar = Calendar.getInstance() ;
        Calendar endCalendar = Calendar.getInstance();
        startCalendar.setTime(start);
        endCalendar.setTime(end);
        return monthDiff(startCalendar, endCalendar) ;

    }

    private long monthDiff(Calendar start,Calendar end){
        assert start != null ;
        assert end != null ;
        long month =(end.getTimeInMillis() - start.getTimeInMillis()) / (3600 * 1000 * 24 * 30l);
        return month ;
    }

}
