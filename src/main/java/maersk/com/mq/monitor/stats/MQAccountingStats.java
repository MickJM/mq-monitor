package maersk.com.mq.monitor.stats;

import java.text.DateFormat;
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
//import maersk.com.mq.monitor.mqmetrics.MQMonitorBase;
import maersk.com.mq.monitor.mqmetrics.MQPCFConstants;

@Component
public class MQAccountingStats {

    private final static Logger log = LoggerFactory.getLogger(MQAccountingStats.class);
    		
	@Autowired
	public MeterRegistry meterRegistry;
    
	//@Autowired
	//private MQMonitorBase base;
	 
    private Map<String,AtomicLong>hourMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>dayMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>weekMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>monthMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>yearMap = new HashMap<String,AtomicLong>();

    private Map<String,AtomicLong>putMaxMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>getMaxMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>putFailMap = new HashMap<String,AtomicLong>();
    private Map<String,AtomicLong>getFailMap = new HashMap<String,AtomicLong>();

    private static final String PUTSHOUR = "mq:puts_per_hour";
    private static final  String GETSHOUR = "mq:gets_per_hour";    
    private static final  String PUTSDAY = "mq:puts_per_day";
    private static final  String GETSDAY = "mq:gets_per_day";
    private static final  String PUTSWEEK = "mq:puts_per_week";
    private static final  String GETSWEEK = "mq:gets_per_week";
    private static final  String PUTSMONTH = "mq:puts_per_month";
    private static final  String GETSMONTH = "mq:gets_per_month";
    private static final  String PUTSYEAR = "mq:puts_per_year";
    private static final String GETSYEAR = "mq:gets_per_year";
    
    private static final String OPENSHOUR = "mq:opens_per_hour";
    private static final String OPENSDAY = "mq:opens_per_day";
    private static final String OPENSWEEK = "mq:opens_per_week";
    private static final String OPENSMONTH = "mq:opens_per_month";
    private static final String OPENSYEAR = "mq:opens_per_year";
    
    private static final String CLOSESHOUR = "mq:closes_per_hour";
    private static final String CLOSESDAY = "mq:closes_per_day";
    private static final String CLOSESWEEK = "mq:closes_per_week";
    private static final String CLOSESMONTH = "mq:closes_per_month";
    private static final String CLOSESYEAR = "mq:closes_per_year";
    
    private String lookupMaxPutMsgSize = "mq:queueMaxPutMsgSize";
    private String lookupMaxGetMsgSize = "mq:queueMaxGetMsgSize";
    private String lookupPutFail = "mq:put_fails";
    private String lookupGetFail = "mq:get_fails";

    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");

    private String queueManagerName;
	public void QueueManagerName(String v) {
		this.queueManagerName = v;
	}
	public String QueueManagerName() {
		return this.queueManagerName;
	}
	
	@Value("${ibm.mq.pcf.period.collections:MONTHS}")
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
	private int collectionTotal = 0;
	public void incrementCollectionTotal(int v) {
		this.collectionTotal += v;
	}
	public int getCollectionTotal() {
		return this.collectionTotal;
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
				incrementCollectionTotal(x);
				
			}
			setSearchCollections(s);
			Arrays.sort(getSearchCollections());	
			
		} 
		log.info("period collections; " + getCollectionString());

	}

	/*
	 * Create or update the appropriate metrics
	 * 
	 */
	public void CreateMetric(AccountingEntity ae) throws ParseException {

		Date dt = formatter.parse(ae.getStartDate() + " " + ae.getStartTime());
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
				
		/*
		 * Hour Of Day
		 */
		switch (ae.getType()) {
		
			case MQConstants.MQIAMO_PUTS:  // Writes
				log.debug("MQIAMO_PUTS");
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					Puts(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					Puts(ae, cal, MQConstants.MQPER_PERSISTENT);
				}
				break;
	
			case MQConstants.MQIAMO_GETS:  // Reads
				log.debug("MQIAMO_GETS");
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					Gets(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					Gets(ae, cal, MQConstants.MQPER_PERSISTENT);
				}
				break;
			
			case MQConstants.MQIAMO_PUT_MAX_BYTES:  // Writes bytes
				log.debug("MQIAMO_MAX_BYTES");
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					PutMaxBytes(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					PutMaxBytes(ae, cal, MQConstants.MQPER_PERSISTENT);
				}
				break;
				
			case MQConstants.MQIAMO_GET_MAX_BYTES:  // Read bytes
				log.debug("MQIAMO_GET_MAX_BYTES");
				if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
					GetMaxBytes(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				}
				if (ae.getValues()[MQConstants.MQPER_PERSISTENT] > 0) {
					GetMaxBytes(ae, cal, MQConstants.MQPER_PERSISTENT);
				}				
				break;

			case MQConstants.MQIAMO_GETS_FAILED:
				log.debug("MQIAMO_FAILED");
				GetsFailures(ae, cal);
				break;
				
			case MQConstants.MQIAMO_PUTS_FAILED:
				log.debug("MQIAMO_FAILED");
				PutsFailures(ae, cal);	
				break;

			case MQConstants.MQIAMO_OPENS:
				log.debug("MQIAMO_OPENS");
				OpenEvents(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				break;

			case MQConstants.MQIAMO_CLOSES:
				log.debug("MQIAMO_CLOSES");
				CloseEvents(ae, cal, MQConstants.MQPER_NOT_PERSISTENT);
				break;
				
			default:
				break;
		}
		
	}

	/*
	 * PUTS
	 */
	private void Puts(AccountingEntity ae, Calendar cal, int per) throws ParseException {

		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = (cal.get(Calendar.MONTH) + 1); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		int year = cal.get(Calendar.YEAR);

		StringBuilder hourLabel = meticsLabel(ae, cal, PUTSHOUR, MQConstants.MQCFUNC_MQPUT, per);		
		long v = 0l;
		String pers = (per == MQConstants.MQPER_PERSISTENT) ? "true" : "false";
		int timePeriods = getCollectionTotal();
		
		/*
		 * Hour
		 *
		*/
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.HOURS) >= 0) {
		if ((timePeriods & MQPCFConstants.HOURS) == MQPCFConstants.HOURS) {		
			AtomicLong put = hourMap.get(hourLabel.toString());
			if (put == null) {
				hourMap.put(hourLabel.toString(), this.meterRegistry.gauge(PUTSHOUR, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.DAYS) >= 0) {
		if ((timePeriods & MQPCFConstants.DAYS) == MQPCFConstants.DAYS) {
			StringBuilder dayLabel = meticsLabel(ae, cal, PUTSDAY, MQConstants.MQCFUNC_MQPUT, per);		
			AtomicLong put = dayMap.get(dayLabel.toString());
			if (put == null) {
				dayMap.put(dayLabel.toString(), this.meterRegistry.gauge(PUTSDAY, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.WEEKS) >= 0) {
		if ((timePeriods & MQPCFConstants.WEEKS) == MQPCFConstants.WEEKS) {
			StringBuilder weekLabel = meticsLabel(ae, cal, PUTSWEEK, MQConstants.MQCFUNC_MQPUT, per);		
			AtomicLong put = weekMap.get(weekLabel.toString());
			if (put == null) {
				weekMap.put(weekLabel.toString(), this.meterRegistry.gauge(PUTSWEEK, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.MONTHS) >= 0) {
		if ((timePeriods & MQPCFConstants.MONTHS) == MQPCFConstants.MONTHS) {	
			StringBuilder monthLabel = meticsLabel(ae, cal, PUTSMONTH, MQConstants.MQCFUNC_MQPUT, per);
			AtomicLong put = monthMap.get(monthLabel.toString());
			if (put == null) {
				monthMap.put(monthLabel.toString(), this.meterRegistry.gauge(PUTSMONTH, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.YEARS) >= 0) {
		if ((timePeriods & MQPCFConstants.YEARS) == MQPCFConstants.YEARS) {
			StringBuilder yearLabel = meticsLabel(ae, cal, PUTSYEAR, MQConstants.MQCFUNC_MQPUT, per);
			AtomicLong put = yearMap.get(yearLabel.toString());
			if (put == null) {
				yearMap.put(yearLabel.toString(), this.meterRegistry.gauge(PUTSYEAR, 
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

	/*
	 * GETS
	 */
	private void Gets(AccountingEntity ae, Calendar cal, int per) throws ParseException {

		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = (cal.get(Calendar.MONTH) + 1); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		int year = cal.get(Calendar.YEAR);

		StringBuilder hourLabel = meticsLabel(ae, cal, GETSHOUR, MQConstants.MQCFUNC_MQGET, per);				
		long v = 0l;
		String pers = (per == MQConstants.MQPER_PERSISTENT) ? "true" : "false";
		int timePeriods = getCollectionTotal();
		
		/*
		 * Hour
		 */
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.HOURS) >= 0) {
		if ((timePeriods & MQPCFConstants.HOURS) == MQPCFConstants.HOURS) {
			AtomicLong get = hourMap.get(hourLabel.toString());
			if (get == null) {
				hourMap.put(hourLabel.toString(), this.meterRegistry.gauge(GETSHOUR, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.DAYS) >= 0) {
		if ((timePeriods & MQPCFConstants.DAYS) == MQPCFConstants.DAYS) {
			StringBuilder dayLabel = meticsLabel(ae, cal, GETSDAY, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = dayMap.get(dayLabel.toString());
			if (get == null) {
				dayMap.put(dayLabel.toString(), this.meterRegistry.gauge(GETSDAY, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.WEEKS) >= 0) {
		if ((timePeriods & MQPCFConstants.WEEKS) == MQPCFConstants.WEEKS) {			
			StringBuilder weekLabel = meticsLabel(ae, cal, GETSWEEK, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = weekMap.get(weekLabel.toString());
			if (get == null) {
				weekMap.put(weekLabel.toString(), this.meterRegistry.gauge(GETSWEEK, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.MONTHS) >= 0) {
		if ((timePeriods & MQPCFConstants.MONTHS) == MQPCFConstants.MONTHS) {			
			StringBuilder monthLabel = meticsLabel(ae, cal, GETSMONTH, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = monthMap.get(monthLabel.toString());
			if (get == null) {
				monthMap.put(monthLabel.toString(), this.meterRegistry.gauge(GETSMONTH, 
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
		//if (Arrays.binarySearch(getSearchCollections(), MQPCFConstants.YEARS) >= 0) {
		if ((timePeriods & MQPCFConstants.YEARS) == MQPCFConstants.YEARS) {			
			StringBuilder yearLabel = meticsLabel(ae, cal, GETSYEAR, MQConstants.MQCFUNC_MQGET, per);		
			AtomicLong get = yearMap.get(yearLabel.toString());
			if (get == null) {
				yearMap.put(yearLabel.toString(), this.meterRegistry.gauge(GETSYEAR, 
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
	private void PutMaxBytes(AccountingEntity ae, Calendar cal, int per) {
		
		AtomicLong putMax = putMaxMap.get(lookupMaxPutMsgSize + "_" + ae.getQueueName() + "_" + per);
		if (putMax == null) {
			putMaxMap.put(lookupMaxPutMsgSize + "_" + ae.getQueueName() + "_" + per, this.meterRegistry.gauge(lookupMaxPutMsgSize, 
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
	private void GetMaxBytes(AccountingEntity ae, Calendar cal, int per) {
		
		AtomicLong getMax = getMaxMap.get(lookupMaxGetMsgSize + "_" + ae.getQueueName() + "_" + per);
		if (getMax == null) {
			getMaxMap.put(lookupMaxGetMsgSize + "_" + ae.getQueueName() + "_" + per, this.meterRegistry.gauge(lookupMaxGetMsgSize, 
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
	private void PutsFailures(AccountingEntity ae, Calendar cal) {

		AtomicLong putFail = putFailMap.get(lookupPutFail + "_" + ae.getQueueManagerName() + "_" + ae.getQueueName());
		if (putFail == null) {
			putFailMap.put(lookupPutFail + "_" + ae.getQueueManagerName() + "_" + ae.getQueueName()
							, this.meterRegistry.gauge(lookupPutFail, 
					Tags.of("queueManagerName", ae.getQueueManagerName(),
							"queueName", ae.getQueueName()							),
					new AtomicLong(ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT]))
					);
		} else {
			long v = putFail.get();
			if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > v) {		
				putFail.set(ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT]);
			}
		}			
	}
	
	/*
	 * Get fails 
	 * 
	 * Value are set in [0] of the int array, just so I dont have to have another parameter
	 * ... [1] is not used for single integer values
	 */
	private void GetsFailures(AccountingEntity ae, Calendar cal) {

		AtomicLong getFail = getFailMap.get(lookupGetFail + "_" + ae.getQueueManagerName() + "_" + ae.getQueueName());
		if (getFail == null) {
			getFailMap.put(lookupGetFail + "_" + ae.getQueueManagerName() + "_" + ae.getQueueName()
							, this.meterRegistry.gauge(lookupGetFail, 
					Tags.of("queueManagerName", ae.getQueueManagerName(),
							"queueName", ae.getQueueName()							),
					new AtomicLong(ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT]))
					);
		} else {
			long v = getFail.get();
			if (ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT] > v) {		
				getFail.set(ae.getValues()[MQConstants.MQPER_NOT_PERSISTENT]);
			}
		}			
		
	}

	/*
	 * Close Events
	 */
	private void CloseEvents(AccountingEntity ae, Calendar cal, int per) throws ParseException {

		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = (cal.get(Calendar.MONTH) + 1); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		int year = cal.get(Calendar.YEAR);

		StringBuilder hourLabel = meticsLabel(ae, cal, CLOSESHOUR, "CLOSE", per);				
		long v = 0l;
		String pers = (per == MQConstants.MQPER_PERSISTENT) ? "true" : "false";
		int timePeriods = getCollectionTotal();
		
		/*
		 * Hour
		 */
		if ((timePeriods & MQPCFConstants.HOURS) == MQPCFConstants.HOURS) {
			AtomicLong get = hourMap.get(hourLabel.toString());
			if (get == null) {
				hourMap.put(hourLabel.toString(), this.meterRegistry.gauge(CLOSESHOUR, 
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
		if ((timePeriods & MQPCFConstants.DAYS) == MQPCFConstants.DAYS) {
			StringBuilder dayLabel = meticsLabel(ae, cal, CLOSESDAY, "CLOSE", per);		
			AtomicLong get = dayMap.get(dayLabel.toString());
			if (get == null) {
				dayMap.put(dayLabel.toString(), this.meterRegistry.gauge(CLOSESDAY, 
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
		if ((timePeriods & MQPCFConstants.WEEKS) == MQPCFConstants.WEEKS) {			
			StringBuilder weekLabel = meticsLabel(ae, cal, CLOSESWEEK, "CLOSE", per);		
			AtomicLong get = weekMap.get(weekLabel.toString());
			if (get == null) {
				weekMap.put(weekLabel.toString(), this.meterRegistry.gauge(CLOSESWEEK, 
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
		if ((timePeriods & MQPCFConstants.MONTHS) == MQPCFConstants.MONTHS) {			
			StringBuilder monthLabel = meticsLabel(ae, cal, CLOSESMONTH, "CLOSE", per);		
			AtomicLong get = monthMap.get(monthLabel.toString());
			if (get == null) {
				monthMap.put(monthLabel.toString(), this.meterRegistry.gauge(CLOSESMONTH, 
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
		if ((timePeriods & MQPCFConstants.YEARS) == MQPCFConstants.YEARS) {			
			StringBuilder yearLabel = meticsLabel(ae, cal, CLOSESYEAR, "CLOSE", per);		
			AtomicLong get = yearMap.get(yearLabel.toString());
			if (get == null) {
				yearMap.put(yearLabel.toString(), this.meterRegistry.gauge(CLOSESYEAR, 
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
	 * Open Events 
	 */
	private void OpenEvents(AccountingEntity ae, Calendar cal, int per) throws ParseException {

		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = (cal.get(Calendar.MONTH) + 1); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		int year = cal.get(Calendar.YEAR);

		StringBuilder hourLabel = meticsLabel(ae, cal, OPENSHOUR, MQConstants.MQCFUNC_MQOPEN, per);				
		long v = 0l;
		String pers = (per == MQConstants.MQPER_PERSISTENT) ? "true" : "false";
		int timePeriods = getCollectionTotal();
		
		/*
		 * Hour
		 */
		if ((timePeriods & MQPCFConstants.HOURS) == MQPCFConstants.HOURS) {
			AtomicLong get = hourMap.get(hourLabel.toString());
			if (get == null) {
				hourMap.put(hourLabel.toString(), this.meterRegistry.gauge(OPENSHOUR, 
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
		if ((timePeriods & MQPCFConstants.DAYS) == MQPCFConstants.DAYS) {
			StringBuilder dayLabel = meticsLabel(ae, cal, OPENSDAY, MQConstants.MQCFUNC_MQOPEN, per);		
			AtomicLong get = dayMap.get(dayLabel.toString());
			if (get == null) {
				dayMap.put(dayLabel.toString(), this.meterRegistry.gauge(OPENSDAY, 
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
		if ((timePeriods & MQPCFConstants.WEEKS) == MQPCFConstants.WEEKS) {			
			StringBuilder weekLabel = meticsLabel(ae, cal, OPENSWEEK, MQConstants.MQCFUNC_MQOPEN, per);		
			AtomicLong get = weekMap.get(weekLabel.toString());
			if (get == null) {
				weekMap.put(weekLabel.toString(), this.meterRegistry.gauge(OPENSWEEK, 
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
		if ((timePeriods & MQPCFConstants.MONTHS) == MQPCFConstants.MONTHS) {			
			StringBuilder monthLabel = meticsLabel(ae, cal, OPENSMONTH, MQConstants.MQCFUNC_MQOPEN, per);		
			AtomicLong get = monthMap.get(monthLabel.toString());
			if (get == null) {
				monthMap.put(monthLabel.toString(), this.meterRegistry.gauge(OPENSMONTH, 
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
		if ((timePeriods & MQPCFConstants.YEARS) == MQPCFConstants.YEARS) {			
			StringBuilder yearLabel = meticsLabel(ae, cal, OPENSYEAR, MQConstants.MQCFUNC_MQOPEN, per);		
			AtomicLong get = yearMap.get(yearLabel.toString());
			if (get == null) {
				yearMap.put(yearLabel.toString(), this.meterRegistry.gauge(OPENSYEAR, 
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
	 * Metric Label
	 */
	private StringBuilder meticsLabel(AccountingEntity ae, Calendar cal, String typeLabel, String mqType, int per) throws ParseException {

		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY); 
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int monthOfYear = (cal.get(Calendar.MONTH) + 1); // Month is indexed from 0 !!, so, JAN = 0, FEB = 1 etc 
		int year = cal.get(Calendar.YEAR);
		
		StringBuilder sb = new StringBuilder();
		switch (typeLabel) {
		
			case PUTSHOUR:
			case GETSHOUR:
			case OPENSHOUR:
			case CLOSESHOUR:
				sb.append(typeLabel);			// Hour label
				sb.append("_");					
				sb.append(ae.getQueueName());	// Queue name
				sb.append("_");
				sb.append(mqType);				// PUT / GET / OPEN / CLOSE
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
			case OPENSDAY:
			case CLOSESDAY:
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
			case OPENSWEEK:
			case CLOSESWEEK:				
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
			case OPENSMONTH:
			case CLOSESMONTH:				
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
			case OPENSYEAR:
			case CLOSESYEAR:				
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
