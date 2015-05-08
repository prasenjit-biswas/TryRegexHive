package com.test;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

public class TestUDF extends UDF {

	@Description(name = "testudf",
			value = "_FUNC_(input) - from the input date string "+
			"or separate month and day arguments, returns the sign of the Zodiac.",
			extended = "Example:\n"
			+ " > SELECT _FUNC_(input_string) FROM src;\n")
	
	
	public String evaluate( String input){
		String result = null;
		if(input != null && !("").equals(input)){
			if("hello".equals(input)){
				result = "firstName";
			}else if("world1".equals(input)){
				result = "secondName";
			}else if("world2".equals(input)){
				result = "secondName";
			}
		}
		return result;
	}
}
