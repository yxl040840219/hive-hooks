package com.customer.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

class SplitInt extends GenericUDF {
	private transient ObjectInspectorConverters.Converter[] converters;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The function SPLIT(s, regexp) takes exactly 2 arguments.");
		}

		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters
					.getConverter(
							arguments[i],
							PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		return ObjectInspectorFactory
				.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 2);

		if (arguments[0].get() == null || arguments[1].get() == null) {
			return null;
		}

		Text s = (Text) converters[0].convert(arguments[0].get());
		Text regex = (Text) converters[1].convert(arguments[1].get());

		ArrayList<IntWritable> result = new ArrayList<IntWritable>();

		for (String str : s.toString().split(regex.toString(), -1)) {
			result.add(new IntWritable(Integer.parseInt(str.trim())));
		}

		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return getStandardDisplayString("split_int", children);
	}

}
