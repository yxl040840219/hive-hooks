package com.customer.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Created by yxl on 2017/12/1.
 */
public class SplitInt extends UDF {

    public ArrayList<Integer> evaluate(final Object content, final String separator){
        ArrayList<Integer> result = new ArrayList<Integer>();
        for (String str : content.toString().split(separator.toString(), -1)) {
            result.add(Double.valueOf(str).intValue());
        }
        return result ;
    }
}
