package maersk.com.mq.monitor.stats;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;

//import org.apache.log4j.Logger;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.ibm.mq.constants.MQConstants;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import maersk.com.mq.monitor.accounting.AccountingEntity;
import maersk.com.mq.monitor.mqmetrics.MQMonitorBase;
import maersk.com.mq.monitor.mqmetrics.MQPCFConstants;

@Component
public class MQAccountingStats {

    //static Logger log = Logger.getLogger(MQAccountingStats.class);
    //static Logger log = LogManager.getLogger(MQAccountingStats.class);
    private final static Logger log = LoggerFactory.getLogger(MQAccountingStats.class);
    		
	@Autowired
	private MQMonitorBase base;
	 
    private Map<String,AtomicLong>hourMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>dayMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>weekMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>monthMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>yearMap = new HashMap<String,AtomicLong>();

    private Map<String,AtomicLong>putMaxMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>getMaxMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>putFailMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>getFailMap = new HashMap<String,AtomicLong>();

    protected static final String hour = "mq:hour";
    protected static final String day = "mq:day";
    protected static final String week = "mq:week";
    protected static final String month = "mq:month";
    protected static final String year = "mq:year";

    protected static final String PUTSHOUR = "mq:puts_per_hour";
    protected static final String GETSHOUR = "mq:gets_per_hour";
    protected static final String PUTSDAY = "mq:puts_per_day";
    protected static final String GETSDAY = "mq:gets_per_day";
    protected static final String PUTSWEEK = "mq:puts_per_week";
    protected static final String GETSWEEK = "mq:gets_per_week";
    protected static final String PUTSMONTH = "mq:puts_per_month";
    protected static final String GETSMONTH = "mq:gets_per_month";
    protected static final String PUTSYEAR = "mq:puts_per_year";
    protected static final String GETSYEAR = "mq:gets_per_year";
    
	protected static final String lookupMaxPutMsgSize = "mq:queueMaxPutMsgSize";
	protected static final String lookupMaxGetMsgSize = "mq:queueMaxGetMsgSize";

	protected static final String lookupPutFail = "mq:put_fails";
	protected static final String lookupGetFail = "mq:get_fails";

    private String queueManagerName;
	public void setQueueManagerName(String v) {
		this.queueManagerName = v;
	}
	public String getQueueManagerName() {
		return this.queueManagerName;
	}
	
	@Value("${ibm.mq.pcf.period.collections:DAYS}")
	private String[] collections;
	private String[] getCollections() {
		return this.collections;
	}
	private int[] searchCollections;
	public void setSearchCollections(int[] v) {
		this.searchCollections = v;
	}
	public int[] getSearchCollections() {
		return this.searchCollections;
	}

	private StringBuilder sb;
	public String getCollectionString() {
		return this.sb.toString();
	}
	
/*
 * 
 *                     Hour        Day       Week      Month       Year
 *                    P    N     P    N     P    N     P    N     P    N  
 * mq:PutCount      999  999   999  999   999  999   999  999   999  999
 * mq:GetCount
 * mq:PutMaxCount
 * mq:GetMaxCount
 * 
 * mq:GetFailure       999         999        999        999        999       
 * mq:PutFailure	   999         999        999        999        999S
 */
	
	@PostConstruct
	public void init() {
		
		if (getCollections() != null) {
			setSearchCollections(new int[getCollections().length]);
			int[] s = new int[getCollections().length];
			int array = 0;

			sb = new StringBuilder();
			for (String w: getCollections()) {
				final int x = MQPCFConstants.getIntValue(w);
				s[array] = x;
				array++;		
				sb.append(w + " ");
			}
			setSearchCollections(s);
			Arrays.sort(getSearchCollections());	
			
			log.info("period collections; " + getCollectionString());
			
		}
	}
	
	public void createMetric(AccountingEntity ae) throws ParseException {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
		Date dt = formatter.parse(ae.getStartDate() + " " + ae.getStartTime());

		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
				
		/*
		 * Hour Of Day
		 */
		switch (ae.getType()) {
		
			case MQConstants.MQIAMO_PUTS:
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					puts(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					puts(ae, cal, MQConstants.MQPER_PERSISTENT);
				}
				break;
	
			case MQConstants.MQIAMO_GETS:
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					gets(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					gets(ae, cal, MQConstants.MQPER_PERSISTENT);
				}
				break;
			
			case MQConstants.MQIAMO_PUT_MAX_BYTES:
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					putMaxBytes(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					putMaxBytes(ae, cal, MQConstants.MQPER_PERSISTENT);
				}
				break;
				
			case MQConstants.MQIAMO_GET_MAX_BYTES:
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					getMaxBytes(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					getMaxBytes(ae, cal, MQConstants.MQPER_PERSISTENT);
				}				
				break;

			case MQConstants.MQIAMO_GETS_FAILED:
				getsFailures(ae, cal);
				break;
				
			case MQConstants.MQIAMO_PUTS_FAILED:
				putsFailures(ae, cal);	
				break;
				
			default:
				break;
		}
		
	}

	/*
	 * PUTS
	 */
	private void puts(AccountingEntity ae, Calendar cal, int per) {

		int[] values = ae.getValues();		
		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = cal.get(Calendar.MONTH);
		int year = cal.get(Calendar.YEAR);

		StringBuilder hourLabel = meticsLabel(ae, cal, PUTSHOUR, MQConstants.MQCFUNC_MQPUT, per);		
		long v = 0l;
		String pers = (per == MQConstants.MQPER_PERSISTENT) ? "true" : "false";
		
		/*
		 * Hour
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.HOURS) >= 0) {
			AtomicLong put = hourMap.get(hourLabel.toString());
			if (put == null) {
				hourMap.put(hourLabel.toString(), base.meterRegistry.gauge(PUTSHOUR, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"hour", String.valueOf(hourOfDay),
								"day",String.valueOf(dayOfMonth),
								"week",String.valueOf(weekOfYear),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (put.get());
				put.set(v);		
			}
		}
		
		/*
		 * Day
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.DAYS) >= 0) {
			StringBuilder dayLabel = meticsLabel(ae, cal, PUTSDAY, MQConstants.MQCFUNC_MQPUT, per);		
			AtomicLong put = dayMap.get(dayLabel.toString());
			if (put == null) {
				dayMap.put(dayLabel.toString(), base.meterRegistry.gauge(PUTSDAY, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"day",String.valueOf(dayOfMonth),
								"week",String.valueOf(weekOfYear),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (put.get());
				put.set(v);
			}
		}
		
		/*
		 * Week
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.WEEKS) >= 0) {
			StringBuilder weekLabel = meticsLabel(ae, cal, PUTSWEEK, MQConstants.MQCFUNC_MQPUT, per);		
			AtomicLong put = weekMap.get(weekLabel.toString());
			if (put == null) {
				weekMap.put(weekLabel.toString(), base.meterRegistry.gauge(PUTSWEEK, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"week",String.valueOf(weekOfYear),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (put.get());
				put.set(v);
			}
		}
		
		/*
		 * Month
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.DAYS) >= 0) {
			StringBuilder monthLabel = meticsLabel(ae, cal, PUTSMONTH, MQConstants.MQCFUNC_MQPUT, per);
			AtomicLong put = monthMap.get(monthLabel.toString());
			if (put == null) {
				monthMap.put(monthLabel.toString(), base.meterRegistry.gauge(PUTSMONTH, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (put.get());
				put.set(v);
			}
		}
		
		/*
		 * Year
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.YEARS) >= 0) {
			StringBuilder yearLabel = meticsLabel(ae, cal, PUTSYEAR, MQConstants.MQCFUNC_MQPUT, per);
			AtomicLong put = yearMap.get(yearLabel.toString());
			if (put == null) {
				yearMap.put(yearLabel.toString(), base.meterRegistry.gauge(PUTSYEAR, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (put.get());
				put.set(v);
			}
		}
	}

	private void gets(AccountingEntity ae, Calendar cal, int per) {

		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = cal.get(Calendar.MONTH);
		int year = cal.get(Calendar.YEAR);

		StringBuilder hourLabel = meticsLabel(ae, cal, GETSHOUR, MQConstants.MQCFUNC_MQGET, per);				
		long v = 0l;
		String pers = (per == MQConstants.MQPER_PERSISTENT) ? "true" : "false";
		
		/*
		 * Hour
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.HOURS) >= 0) {
			AtomicLong get = hourMap.get(hourLabel.toString());
			if (get == null) {
				hourMap.put(hourLabel.toString(), base.meterRegistry.gauge(GETSHOUR, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"hour", String.valueOf(hourOfDay),
								"day",String.valueOf(dayOfMonth),
								"week",String.valueOf(weekOfYear),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (get.get());
				get.set(v);
			}		
		}
		/*
		 * Day
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.DAYS) >= 0) {
			StringBuilder dayLabel = meticsLabel(ae, cal, GETSDAY, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = dayMap.get(dayLabel.toString());
			if (get == null) {
				dayMap.put(dayLabel.toString(), base.meterRegistry.gauge(GETSDAY, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"day",String.valueOf(dayOfMonth),
								"week",String.valueOf(weekOfYear),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (get.get());
				get.set(v);
			}
		}
		
		/*
		 * Week
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.WEEKS) >= 0) {
			StringBuilder weekLabel = meticsLabel(ae, cal, GETSWEEK, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = weekMap.get(weekLabel.toString());
			if (get == null) {
				weekMap.put(weekLabel.toString(), base.meterRegistry.gauge(GETSWEEK, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"week",String.valueOf(weekOfYear),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (get.get());
				get.set(v);
			
			}
		}
		
		/*
		 * Month
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.MONTHS) >= 0) {
			StringBuilder monthLabel = meticsLabel(ae, cal, GETSMONTH, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = monthMap.get(monthLabel.toString());
			if (get == null) {
				monthMap.put(monthLabel.toString(), base.meterRegistry.gauge(GETSMONTH, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"month",String.valueOf(monthOfYear),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (get.get());
				get.set(v);
			
			}
		}
		
		/*
		 * Year
		 */
		if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.YEARS) >= 0) {
			StringBuilder yearLabel = meticsLabel(ae, cal, GETSYEAR, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = yearMap.get(yearLabel.toString());
			if (get == null) {
				yearMap.put(yearLabel.toString(), base.meterRegistry.gauge(GETSYEAR, 
						Tags.of("queueManagerName", ae.getQueueManagerName(),
								"queueName", ae.getQueueName(),
								"year",String.valueOf(year),
								"persistence", pers							
								),
						new AtomicLong(ae.getValues()[per]))
						);
			} else {
				v = ae.getValues()[per] + (get.get());
				get.set(v);
			}		
		}
	}
	
	/*
	 * MAX PUT Value
	 */
	private void putMaxBytes(AccountingEntity ae, Calendar cal, int per) {
		
		AtomicLong putMax = putMaxMap.get(lookupMaxPutMsgSize + "_" + ae.getQueueName() + "_" + per);
		if (putMax == null) {
			putMaxMap.put(lookupMaxPutMsgSize + "_" + ae.getQueueName() + "_" + per, base.meterRegistry.gauge(lookupMaxPutMsgSize, 
					Tags.of("queueManagerName", ae.getQueueManagerName(),
							"queueName", ae.getQueueName()							),
					new AtomicLong(ae.getValues()[per]))
					);
		} else {
			long v = putMax.get();
			if (ae.getValues()[per] > v) {		
				putMax.set(ae.getValues()[per]);
			}
		}			
	}

	/*
	 * MAX PUT Value
	 */
	private void getMaxBytes(AccountingEntity ae, Calendar cal, int per) {
		
		AtomicLong getMax = getMaxMap.get(lookupMaxGetMsgSize + "_" + ae.getQueueName() + "_" + per);
		if (getMax == null) {
			getMaxMap.put(lookupMaxGetMsgSize + "_" + ae.getQueueName() + "_" + per, base.meterRegistry.gauge(lookupMaxGetMsgSize, 
					Tags.of("queueManagerName", ae.getQueueManagerName(),
							"queueName", ae.getQueueName()							),
					new AtomicLong(ae.getValues()[per]))
					);
		} else {
			//long v = ae.getValues()[per] + (getMax.get());
			long v = getMax.get();
			if (ae.getValues()[per] > v) {		
				getMax.set(ae.getValues()[per]);
			}
		}			
	}

	/*
	 * Put fails
	 */
	private void putsFailures(AccountingEntity ae, Calendar cal) {

		AtomicLong putFail = putFailMap.get(lookupPutFail + "_" + ae.getQueueName());
		if (putFail == null) {
			getMaxMap.put(lookupPutFail + "_" + ae.getQueueName(), base.meterRegistry.gauge(lookupPutFail, 
					Tags.of("queueManagerName", ae.getQueueManagerName(),
							"queueName", ae.getQueueName()							),
					new AtomicLong(ae.getValues()[0]))
					);
		} else {
			long v = putFail.get();
			if (ae.getValues()[0] > v) {		
				putFail.set(ae.getValues()[0]);
			}
		}			
	}
	
	/*
	 * Get fails 
	 * 
	 * Value are set in [0] of the int array, just so I dont have to have another parameter
	 * ... [1] is not used for single integer values
	 */
	private void getsFailures(AccountingEntity ae, Calendar cal) {

		AtomicLong getFail = getFailMap.get(lookupGetFail + "_" + ae.getQueueName());
		if (getFail == null) {
			getMaxMap.put(lookupGetFail + "_" + ae.getQueueName(), base.meterRegistry.gauge(lookupGetFail, 
					Tags.of("queueManagerName", ae.getQueueManagerName(),
							"queueName", ae.getQueueName()							),
					new AtomicLong(ae.getValues()[0]))
					);
		} else {
			long v = getFail.get();
			if (ae.getValues()[0] > v) {		
				getFail.set(ae.getValues()[0]);
			}
		}			
		
	}
	
	/*
	 * Metric Label
	 */
	private StringBuilder meticsLabel(AccountingEntity ae, Calendar cal, String typeLabel, String mqType, int per) {

		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = cal.get(Calendar.MONTH);
		int year = cal.get(Calendar.YEAR);
		
		StringBuilder sb = new StringBuilder();
		switch (typeLabel) {
		
			case PUTSHOUR:
			case GETSHOUR:
				sb.append(typeLabel);			// Hour label
				sb.append("_");					
				sb.append(ae.getQueueName());	// Queue name
				sb.append("_");
				sb.append(mqType);				// PUT / GET 
				sb.append("_");
				sb.append(per);					// persistence 
				sb.append("_");
				sb.append(hourOfDay);			// Hour when the put was issued
				sb.append("_");
				sb.append(dayOfMonth);			// Day when the put was issued
				sb.append("_");
				sb.append(weekOfYear);			// Week of the year
				sb.append("_");
				sb.append(monthOfYear);			// Month
				sb.append("_");
				sb.append(year);				// Year
				break;
				
			case PUTSDAY:
			case GETSDAY:
				sb.append(typeLabel);			// Day label
				sb.append("_");					
				sb.append(ae.getQueueName());	// Queue name
				sb.append("_");
				sb.append(mqType);				// PUT / GET		
				sb.append("_");
				sb.append(per);					// persistence 
				sb.append("_");
				sb.append(dayOfMonth);			// Day when the put was issued
				sb.append("_");
				sb.append(weekOfYear);			// Week of the year
				sb.append("_");
				sb.append(monthOfYear);			// Month
				sb.append("_");
				sb.append(year);				// Year
				break;
				
			case PUTSWEEK:
			case GETSWEEK:				
				sb.append(typeLabel);			// Week label
				sb.append("_");					
				sb.append(ae.getQueueName());	// Queue name
				sb.append("_");
				sb.append(mqType);				// PUT / GET		
				sb.append("_");
				sb.append(per);					// persistence 
				sb.append("_");
				sb.append(weekOfYear);			// Week of the year
				sb.append("_");
				sb.append(monthOfYear);			// Month
				sb.append("_");
				sb.append(year);				// Year
				break;

			case PUTSMONTH:
			case GETSMONTH:				
				sb.append(typeLabel);			// Month label
				sb.append("_");					
				sb.append(ae.getQueueName());	// Queue name
				sb.append("_");
				sb.append(mqType);				// PUT / GET		
				sb.append("_");
				sb.append(per);					// persistence 
				sb.append("_");
				sb.append(monthOfYear);			// Month
				sb.append("_");
				sb.append(year);				// Year
				break;

			case PUTSYEAR:
			case GETSYEAR:				
				sb.append(typeLabel);			// Year label
				sb.append("_");					
				sb.append(ae.getQueueName());	// Queue name
				sb.append("_");
				sb.append(mqType);				// PUT / GET		
				sb.append("_");
				sb.append(per);					// persistence 
				sb.append("_");
				sb.append(year);				// Year
				break;

			default:
				break;
		}

		return sb;
	
	}
	
}
