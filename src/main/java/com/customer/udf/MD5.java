package com.customer.udf;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * Hive自定义UDF，计算md5值
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
