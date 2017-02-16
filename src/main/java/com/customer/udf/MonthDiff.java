package com.customer.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

class MonthDiff extends GenericUDF {
	private transient ObjectInspectorConverters.Converter[] converters;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The function monthdiff(start,end) takes exactly 2 arguments.");
		}

		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters
					.getConverter(
							arguments[i],
							PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 2);

		if (arguments[0].get() == null || arguments[1].get() == null) {
			return null;
		}

		Text start = (Text) converters[0].convert(arguments[0].get());
		Text end = (Text) converters[1].convert(arguments[1].get());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM") ;
		Calendar startCalendar = Calendar.getInstance() ;
		Calendar endCalendar = Calendar.getInstance();
		try {
			startCalendar.setTime(sdf.parse(start.toString().trim())) ;
			endCalendar.setTime(sdf.parse(end.toString().trim()));
		} catch (ParseException e) {
			throw new HiveException("日期解析错误 yyyy-MM") ;
		}
		long month =(endCalendar.getTimeInMillis() - startCalendar.getTimeInMillis()) / (3600 * 1000 * 24 * 30l);
		LongWritable result = new LongWritable(month);
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return getStandardDisplayString("monthdiff", children);
	}

	public static void main(String[] args) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM") ;
		Calendar startCalendar = Calendar.getInstance() ;
		startCalendar.setTime(sdf.parse("2015-03")) ;
		System.out.println(startCalendar.getTime() + "\t" + startCalendar.getTimeInMillis());
		Calendar endCalendar = Calendar.getInstance();
		endCalendar.setTime(sdf.parse("2015-03"));
		System.out.println(endCalendar.getTime() + "\t" + endCalendar.getTimeInMillis());
		long month =(endCalendar.getTimeInMillis() - startCalendar.getTimeInMillis()) / (3600 * 1000 * 24 * 30l);   
		System.out.println(month);
	}
	
}
