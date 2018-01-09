package com.customer.deprecate;

import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Map;

/**
 * Hive自定义UDF，计算md5值
 * hadoop fs -copyFromLocal hive-custom-0.0.1.jar /data/hive/udf/
 * create function md5 as "com.customer.deprecate.MD5" using jar "hdfs://hadoop-cluster/data/hive/udf/hive-custom-0.0.1.jar" ;
 * select md5(id,name) from t_test ;
 */
public class MD5 extends GenericUDF {


    private transient ObjectInspector[] argumentOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {
        argumentOIs = arguments ;
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector ;
    }


    /**
     * @param arguments 传入参数
     * @return md5值
     */
    public Object evaluate(DeferredObject[] arguments)  throws HiveException {
        StringBuffer sb = new StringBuffer() ;
        for (int i = 0; i < arguments.length; i++) {
             sb.append(_md5(arguments[i].get(), argumentOIs[i])) ;
        }
        Text result = new Text(Hashing.md5().hashString(sb.toString()).toString());
        return  result ;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("md5", children, ",");
    }



    public static Object _md5(Object o, ObjectInspector objIns) {
        if (o == null) {
            return "";
        }
        switch (objIns.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objIns);
                switch (poi.getPrimitiveCategory()) {
                    case VOID:
                        return "" ;
                    case BOOLEAN:
                        return ((BooleanObjectInspector) poi).get(o) ;
                    case BYTE:
                        return ((ByteObjectInspector) poi).get(o);
                    case SHORT:
                        return ((ShortObjectInspector) poi).get(o);
                    case INT:
                        return ((IntObjectInspector) poi).get(o);
                    case LONG:
                        return ((LongObjectInspector) poi).get(o);
                    case FLOAT:
                        return ((FloatObjectInspector) poi).get(o) ;
                    case DOUBLE:
                        return ((DoubleObjectInspector) poi).get(o);
                    case STRING:
                        return ((StringObjectInspector) poi).getPrimitiveWritableObject(o);
                    case CHAR:
                        return ((HiveCharObjectInspector) poi).getPrimitiveWritableObject(o) ;
                    case VARCHAR:
                        return ((HiveVarcharObjectInspector)poi).getPrimitiveWritableObject(o);
                    case BINARY:
                        return ((BinaryObjectInspector) poi).getPrimitiveWritableObject(o);
                    case DATE:
                        return ((DateObjectInspector) poi).getPrimitiveWritableObject(o);
                    case TIMESTAMP:
                        return ((TimestampObjectInspector) poi).getPrimitiveWritableObject(o);
                    case INTERVAL_YEAR_MONTH:
                        return ((HiveIntervalYearMonthObjectInspector) poi)
                                .getPrimitiveWritableObject(o);
                    case INTERVAL_DAY_TIME:
                        return ((HiveIntervalDayTimeObjectInspector) poi)
                                .getPrimitiveWritableObject(o);
                    case DECIMAL:
                        return ((HiveDecimalObjectInspector) poi).getPrimitiveWritableObject(o);
                    default: {
                        throw new RuntimeException("Unknown type: "
                                + poi.getPrimitiveCategory());
                    }
                }
            }
            case LIST: {
                ListObjectInspector listOI = (ListObjectInspector)objIns;
                ObjectInspector elemOI = listOI.getListElementObjectInspector();
                StringBuffer sb = new StringBuffer();
                for (int ii = 0; ii < listOI.getListLength(o); ++ii) {
                    sb.append(_md5(listOI.getListElement(o, ii), elemOI));
                }
                return sb ;
            }
            case MAP: {
                StringBuffer sb = new StringBuffer();
                MapObjectInspector mapOI = (MapObjectInspector)objIns;
                ObjectInspector keyOI = mapOI.getMapKeyObjectInspector();
                ObjectInspector valueOI = mapOI.getMapValueObjectInspector();
                Map<?, ?> map = mapOI.getMap(o);
                for (Map.Entry<?,?> entry : map.entrySet()) {
                    sb.append(_md5(entry.getKey(), keyOI))
                            .append(_md5(entry.getValue(), valueOI));
                }
                return sb ;
            }
            case STRUCT:
                StringBuffer sb = new StringBuffer();
                StructObjectInspector structOI = (StructObjectInspector)objIns;
                List<? extends StructField> fields = structOI.getAllStructFieldRefs();
                for (StructField field : fields) {
                    sb.append(_md5(structOI.getStructFieldData(o, field),
                            field.getFieldObjectInspector()));
                }
                return sb;

            case UNION:
                UnionObjectInspector uOI = (UnionObjectInspector)objIns;
                byte tag = uOI.getTag(o);
                return _md5(uOI.getField(o), uOI.getObjectInspectors().get(tag));

            default:
                throw new RuntimeException("Unknown type: "+ objIns.getTypeName());
        }
    }



}
