package maersk.com.mq.monitor.mqmetrics;

/*
 * Copyright 2019
 * Maersk
 *
 * Connect to a queue manager
 * 
 * 22/10/2019 - Capture the return code when the queue manager throws an error so multi-instance queue
 *              managers can be checked
 */

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

//import org.apache.logging.log4j.Logger;
//import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQExceptionWrapper;
import com.ibm.mq.headers.pcf.PCFMessageAgent;
import com.ibm.mq.headers.pcf.PCFException;
import maersk.com.mq.monitor.accounting.*;
import maersk.com.mq.monitor.stats.*;
import maersk.com.mq.json.controller.JSONController;

@Component
public class MQConnection {

    private final static Logger log = LoggerFactory.getLogger(MQConnection.class);

	@Value("${application.save.metrics.required:false}")
    private boolean summaryRequired;

	@Value("${ibm.mq.multiInstance:false}")
	private boolean multiInstance;
	public boolean isMultiInstance() {
		return this.multiInstance;
	}
	
	@Value("${ibm.mq.queueManager}")
	private String queueManager;
	private String getQueueManagerName() {
		return this.queueManager;
	}

	@Value("${ibm.mq.user:#{null}}")
	private String userID;
	private String getUserId() {
		return this.userID;
	}

	@Value("${ibm.mq.local:false}")
	private boolean local;
	public boolean isRunningLocal() {
		return this.local;
	}
	
	@Value("${ibm.mq.keepMetricsWhenQueueManagerIsDown:false}")
	private boolean keepMetricsWhenQueueManagerIsDown;
	
	//
	@Value("${ibm.mq.useSSL:false}")
	private boolean bUseSSL;
	public boolean usingSSL() {
		return this.bUseSSL;
	}
	
	@Value("${ibm.mq.security.truststore:}")
	private String truststore;
	@Value("${ibm.mq.security.truststore-password:}")
	private String truststorepass;
	@Value("${ibm.mq.security.keystore:}")
	private String keystore;
	@Value("${ibm.mq.security.keystore-password:}")
	private String keystorepass;
	
    @Value("${ibm.mq.event.delayInMilliSeconds:10000}")
    private long resetIterations;

    private MQQueueManager queManager = null;
    public MQQueueManager getMQQueueManager() {
    	return this.queManager;
    }
    private void setMQQueueManager(MQQueueManager v) {
    	this.queManager = v;
    }
    
    private PCFMessageAgent messageAgent = null;
    private PCFMessageAgent getMessageAgent() {
    	return this.messageAgent;
    }
    private void setMessageAgent(PCFMessageAgent v) {
    	this.messageAgent = v;
    }

    private long numberOfMessagesProcessed;
    public long getNumberOfMessagesProcessed() {
    	return this.numberOfMessagesProcessed;
    }
    public void incrementNumberOfMessagesProcessed(long v) {
    	this.numberOfMessagesProcessed = v;
    }
    public void incrementNumberOfMessagesProcessed() {
    	this.numberOfMessagesProcessed++;
    }
    
	@Autowired
	private MQQueueManagerStats qmStats;
	private MQQueueManagerStats getQMStatsObject() {
		return this.qmStats;
	}

    private int reasonCode;
    private void saveReasonCode(int v) {
    	this.reasonCode = v;
    }
    public int getReasonCode() {
    	return this.reasonCode;
    }
	
	@Autowired
	private MQMonitorBase base;

    @Autowired
    public MQMetricsQueueManager mqMetricsQueueManager;
    private MQMetricsQueueManager getMQMetricQueueManager() {
    	return this.mqMetricsQueueManager;
    }

    
	@Autowired
	private MQAccountingStats qmAcctStats;
	public MQAccountingStats getAccountingStats() {
		return this.qmAcctStats;
	}
    
    @Bean
    public JSONController JSONController() {
    	return new JSONController();
    }
    
    // Constructor
	public MQConnection() {
	}
	
	@PostConstruct
	public void setProperties() throws MQException, MQDataException, IOException {
		
		log.info("MQConnection: Object created");
		
		/*
		 * Make a connection to the queue manager
		 */
		//connectToQueueManager();
		/*
		if (getMessageAgent() != null) {
			getQMStatsObject().setQueueManagerName(queueManager);
			getAccountingStats().setQueueManagerName(queueManager);

			getQMStatsObject().setRunMode(MQPCFConstants.MODE_CLIENT);
			getQMStatsObject().setVersion();
		}
		*/
		
		incrementNumberOfMessagesProcessed(0);
		
	}
	
	/*
	 * Every 'x' seconds, start the processing to get the MQ metrics
	 * 
	 * Main loop
	 *    if we have a messageAgent object
	 *        call 'getMetrics'
	 *            
	 *    if not
	 *        call 'connectToQueueManager'
	 *    
	 */
	@Scheduled(fixedDelayString="${ibm.mq.event.delayInMilliSeconds}")
    public void scheduler() {
	
		try {
			if (getMessageAgent() != null) {
				getMetrics();
				
			} else {
				connectToQueueManager();
				
			}
			
		} catch (PCFException p) {
			log.error("PCFException " + p.getMessage());
			log.error("PCFException: ReasonCode " + p.getReason());
			if (log.isTraceEnabled()) { p.printStackTrace(); }
			closeQMConnection(p.getReason());
			getQMStatsObject().connectionBroken(p.reasonCode);
			queueManagerIsNotRunning(p.getReason());
			
		} catch (MQException m) {
			log.error("MQException " + m.getMessage());
			log.error("MQException: ReasonCode " + m.getReason());
			if (log.isTraceEnabled()) { m.printStackTrace(); }
			closeQMConnection(m.getReason());
			getQMStatsObject().connectionBroken(m.reasonCode);
			queueManagerIsNotRunning(m.getReason());
	    	setMessageAgent(null);

		} catch (MQExceptionWrapper w) {
			log.error("MQException " + w.getMessage());
			log.error("MQException: ReasonCode " + w.getReason());
			if (log.isTraceEnabled()) { w.printStackTrace(); }
			closeQMConnection(w.getReason());
			getQMStatsObject().connectionBroken(w.reasonCode);
			queueManagerIsNotRunning(w.getReason());
	    	setMessageAgent(null);

		} catch (IOException i) {
			log.error("IOException " + i.getMessage());
			if (log.isTraceEnabled()) { i.printStackTrace(); }
			closeQMConnection();
			getQMStatsObject().connectionBroken();
			queueManagerIsNotRunning(MQPCFConstants.PCF_INIT_VALUE);
			
		} catch (Exception e) {
			log.error("Exception " + e.getMessage());
			if (log.isTraceEnabled()) { e.printStackTrace(); }
			closeQMConnection();
			getQMStatsObject().connectionBroken();
			queueManagerIsNotRunning(MQPCFConstants.PCF_INIT_VALUE);
		}
    }    
	
	/*
	 * Connect to the queue manager
	 */
	private void connectToQueueManager() throws MQException, MQDataException, IOException {
		
		log.warn("No MQ queue manager object");
		createQueueManagerConnection();

		getQMStatsObject().connectionBroken();
	}
	
	/*
	 * Create an MQ connection to the queue manager
	 * ... once connected, 
	 *    create a messageAgent for PCF commands
	 *    ensure queue manager names are correctly set
	 *    calc start/end dates
	 *    open queue for reading
	 *    set queue manager status
	 * 
	 */
	public void createQueueManagerConnection() throws MQException, MQDataException, IOException {
		
		whichAuthentication();

	/*	
		if (getMessageAgent() != null) {
			getQMStatsObject().setQueueManagerName(queueManager);
			getAccountingStats().setQueueManagerName(queueManager);

			getQMStatsObject().setRunMode(MQPCFConstants.MODE_CLIENT);
			getQMStatsObject().setVersion();
		}
	*/

		setMQQueueManager(getMQMetricQueueManager().createQueueManager());
		setMessageAgent(getMQMetricQueueManager().createMessageAgent(getMQQueueManager()));
		
		getMQMetricQueueManager().setQueueManager(getQueueManagerName());
		getQMStatsObject().setQueueManagerName(getQueueManagerName());
		getAccountingStats().setQueueManagerName(getQueueManagerName());

		if (isRunningLocal()) {
			getQMStatsObject().setRunMode(MQPCFConstants.MODE_LOCAL);
			
		} else {
			getQMStatsObject().setRunMode(MQPCFConstants.MODE_CLIENT);
			
		}
		getQMStatsObject().setVersion();

		//getMQMetricQueueManager().getQueueManagerMonitoringValues();		
		getMQMetricQueueManager().calculateStartEndDates();
		
		getMQMetricQueueManager().setQueue(null);		
		getMQMetricQueueManager().openQueueForReading();
		//getQMStatsObject().queueManagerStatus(getMQMetricQueueManager().getQueueManagerStatus());

	}
		
	/*
	 * Check authentication method ... user or certificates
	 */
	private void whichAuthentication() {
		
		if (getUserId() != null && (!getUserId().isEmpty())) {
			if (usingSSL()) {
				log.info("Authentication using TLS certificates");
				
			} else {
				log.info("Authentication using username and password");
				
			}
		} else {
			log.info("Authentication using TLS certificates");
			
		}
		
	}
	
	/*
	 * When the queue manager isn't running, send back a status of inactive 
	 */
	private void queueManagerIsNotRunning(int status) {

		getQMStatsObject().QueueManagerNotRunning(status);

	}

	/*
	 * Get metrics
	 */
	public void getMetrics() throws PCFException, MQException, 
			IOException, MQDataException, ParseException {
		
		getMQMetricQueueManager().getQueueManagerMonitoringValues();		
		getQMStatsObject().queueManagerStatus(getMQMetricQueueManager().getQueueManagerStatus());

		processAccountingMetrics();
		
	}

	/*
	 * Get a list of AccountityEntity objects;
	 * Loop through each returned object
	 */
	private void processAccountingMetrics() throws MQDataException, IOException, ParseException, MQException {

		List<AccountingEntity> list = getMQMetricQueueManager().readAccountData();		
		if (!list.isEmpty()) {
			for (AccountingEntity ae : list) {
				getAccountingStats().createMetric(ae);				
				incrementNumberOfMessagesProcessed();
			}
		}
	}
		
	/*
	 * Disconnect cleanly from the queue manager
	 */
    @PreDestroy
    public void disconnect() {
    	closeQMConnection();
    }
    
    /*
     * Disconnect, showing the reason
     */
    public void closeQMConnection(int reasonCode) {

		log.info("Disconnected from the queue manager"); 
		log.info("Reason code: " + reasonCode);
		getMQMetricQueueManager().CloseConnection(getMQQueueManager(), getMessageAgent());
    	setMQQueueManager(null);
    	setMessageAgent(null);
		
    }
	        
    public void closeQMConnection() {

		log.info("Disconnected from the queue manager"); 
		getMQMetricQueueManager().CloseConnection(getMQQueueManager(), getMessageAgent());
    	setMQQueueManager(null);
    	setMessageAgent(null);

    }
}


