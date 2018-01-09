package com.customer.deprecate;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * 
 */
public class CastString extends UDF {

    public String evaluate(final Object o) {
        		if(o == null){
        			 return "" ;
        		}else{
        			return String.valueOf(o);
        		}
    }
}
