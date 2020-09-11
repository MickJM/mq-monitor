package maersk.com.mq.monitor.mqmetrics;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.aspectj.weaver.patterns.ThisOrTargetAnnotationPointcut;

public interface MQPCFConstants {

	final public int PCF_INIT_VALUE = 0;
	final public int MODE_LOCAL = 0;
	final public int MODE_CLIENT = 1;

	final public int EXIT_ERROR = 2;
		
	final public int HOURS = 1;
	final public int DAYS = 2;
	final public int WEEKS = 4;
	final public int MONTHS = 8;
	final public int YEARS = 16;
	
	final public int ERROR_IO_EXCEPTION = 9998;
	final public int ERROR_EXCEPTION = 9997;
	
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
