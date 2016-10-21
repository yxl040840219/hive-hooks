package com.customer.udf;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * Hive自定义UDF，计算md5值
 * hadoop fs -copyFromLocal hive-custom-0.0.1.jar /data/hive/udf/
 * create function md5 as "com.customer.udf.MD5" using jar "hdfs://hadoop-cluster/data/hive/udf/hive-custom-0.0.1.jar" ;
 * select md5(concat(id,name)) from t_test ;
 */
public class MD5 extends UDF {
    /**
     * @param s 传入参数
     * @return md5值
     */
    public String evaluate(final String s) {
        if(Strings.isNullOrEmpty(s.trim())){
            return null;
        }
        return Hashing.md5().hashString(s.trim()).toString();
    }
}
