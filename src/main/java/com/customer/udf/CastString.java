package com.customer.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
/**
 *转换多个列为String 
 */
public class CastString extends GenericUDF {
    
	/**
	 * 这个方法的目标是确定函数的参数类型，如果参数的数量以及类型不合法，要抛出异常以给用户必要的提示
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
	    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;  
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		StringBuffer sb = new StringBuffer();
		for(DeferredObject o:arguments){
			Object value = o.get();
			if(value == null){
				sb.append("");
			}else{
				sb.append(String.valueOf(value));
			}
		}
		return sb.toString();
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();  
        sb.append("castString(");  
        if (children.length > 0) {  
            sb.append(children[0]);  
            for (int i = 1; i < children.length; i++) {  
                sb.append(",");  
                sb.append(children[i]);  
            }  
        }  
        sb.append(")");  
        return sb.toString();  

	}

   
}
