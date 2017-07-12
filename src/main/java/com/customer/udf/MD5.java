package com.customer.udf;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * Hive自定义UDF，计算md5值
 * hadoop fs -copyFromLocal hive-custom-0.0.1.jar /data/hive/udf/
 * create function md5 as "com.customer.udf.MD5" using jar "hdfs://hadoop-cluster/data/hive/udf/hive-custom-0.0.1.jar" ;
 * select md5(concat(id,name)) from t_test ;
 */
public class MD5 extends GenericUDF {


    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters
                    .getConverter(
                            arguments[i],
                            PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector ;
    }


    /**
     * @param arguments 传入参数
     * @return md5值
     */
    public Object evaluate(DeferredObject[] arguments)  throws HiveException {
        StringBuffer sb = new StringBuffer() ;
        for (int i = 0; i < arguments.length; i++) {
             sb.append(converters[i].convert(arguments[i].get())) ;
        }
        Text result = new Text(Hashing.md5().hashString(sb.toString()).toString());
        return  result ;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("md5", children, ",");
    }


    public static void main(String[] args) throws Exception {
        MD5 md5 = new MD5();
        Integer [] arguments = {123,123} ;
        StringBuffer sb = new StringBuffer() ;
        for (int i = 0; i < arguments.length; i++) {
            sb.append(arguments[i].toString()) ;
        }
        System.out.println(Hashing.md5().hashString(sb.toString()).toString());
    }
}
