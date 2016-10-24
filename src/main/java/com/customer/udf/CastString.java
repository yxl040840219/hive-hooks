package com.customer.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 *转换多个列为String 
 */
public class CastString extends UDF {

    public String evaluate(final Object... os) {
    	   StringBuffer sb = new StringBuffer();
        for(Object o:os){
        		if(os == null){
        			sb.append("") ;
        		}else{
        			sb.append(String.valueOf(o)) ;
        		}
        }
        return sb.toString();
    }
	
}
