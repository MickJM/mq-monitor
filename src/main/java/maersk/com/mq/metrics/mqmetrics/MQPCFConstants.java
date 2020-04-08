package maersk.com.mq.metrics.mqmetrics;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.aspectj.weaver.patterns.ThisOrTargetAnnotationPointcut;

public interface MQPCFConstants {

	//final public int NOTSET = -1;
	//final public int MULTIINSTANCE = 1;

	//final public int BASE = 0;
	final public int PCF_INIT_VALUE = 0;
	//final public int NOT_MULTIINSTANCE = 0;
	//final public int MODE_LOCAL = 0;
	final public int MODE_CLIENT = 1;
	final public int EXIT_ERROR = 2;
		
	final public int NONE = 10;
	final public int INFO = 11;
	final public int DEBUG = 12;
	final public int WARN = 14;
	final public int ERROR = 18;
	final public int TRACE = 116;
	
	final public int HOURS = 21;
	final public int DAYS = 22;
	final public int WEEKS = 24;
	final public int MONTHS = 28;
	final public int YEARS = 216;
	
	public static int getIntValue(String w) {
		
		int ret = 0;
		switch (w) {
		
			case "HOURS":
				ret = HOURS;
				break;
			case "DAYS":
				ret = DAYS;
				break;
			case "WEEKS":
				ret = WEEKS;
				break;
			case "MONTHS":
				ret = MONTHS;
				break;
			case "YEARS":
				ret = YEARS;
				break;				
			default:
				break;
				
		}
		return ret;
	}
	

		
}
