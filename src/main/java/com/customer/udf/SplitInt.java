package com.customer.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

/**
 * Created by yxl on 2017/12/1.
 */
public class SplitInt extends UDF {

    public ArrayWritable evaluate(final Object content, final String separator){
        ArrayWritable arrayWritable = new ArrayWritable(IntWritable.class) ;
        ArrayList<IntWritable> result = new ArrayList<IntWritable>();
        for (String str : content.toString().split(separator.toString(), -1)) {
            IntWritable intWritable = new IntWritable();
            intWritable.set(Double.valueOf(str).intValue());
            result.add(intWritable);
        }
        IntWritable [] writables = new IntWritable[result.size()] ;
        arrayWritable.set(result.toArray(writables));
        return arrayWritable ;
    }
}
