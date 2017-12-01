package com.customer.udf;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Created by yxl on 2017/12/1.
 */
public class SplitIntUDF extends UDF {

    public Integer [] evaluate(Object content,String separator){
        ArrayList<Integer> result = new ArrayList<Integer>();
        for (String str : content.toString().split(separator.toString(), -1)) {
            result.add(Integer.parseInt(str));
        }
        return result.toArray(new Integer[0]) ;
    }

    public static void main(String[] args){
        Integer [] a = new SplitIntUDF().evaluate("1,2,3,4",",") ;
        System.out.println(ArrayUtils.toString(a));
    }
}
