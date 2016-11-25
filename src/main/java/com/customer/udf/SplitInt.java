package com.customer.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

class SplitInt extends UDF{
	
    public ArrayList<IntWritable> evaluate(final String arg1,final String arg2) {

    	    ArrayList<IntWritable> result = new ArrayList<IntWritable>();

    	    for (String str : arg1.toString().split(arg2.toString(), -1)) {
    	      result.add(new IntWritable(Integer.parseInt(str)));
    	    }
    	    
    	    return result;
}
	
}