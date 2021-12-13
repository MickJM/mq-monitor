package monitor.mq.monitor.mqmetrics;

/*
 * Copyright 2020
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQExceptionWrapper;
import com.ibm.mq.headers.pcf.PCFMessageAgent;

import monitor.mq.json.controller.JSONController;
import monitor.mq.monitor.accounting.*;
import monitor.mq.monitor.stats.*;

import com.ibm.mq.headers.pcf.PCFException;

@Component
public class MQConnection {

    private final static Logger log = LoggerFactory.getLogger(MQConnection.class);

	@Value("${ibm.mq.queueManager}")
	private String queuemanagername;
	private String QueueManagerName() {
		return this.queuemanagername;
	}

	@Value("${ibm.mq.user:#{null}}")
	private String userid;
	private String UserId() {
		return this.userid;
	}

	@Value("${ibm.mq.local:false}")
	private boolean local;
	public boolean RunningLocal() {
		return this.local;
	}
	
	@Value("${ibm.mq.useSSL:false}")
	private boolean usessl;
	public boolean UseSSL() {
		return this.usessl;
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

    private MQQueueManager quemanager = null;
    public MQQueueManager QueueManagerObject() {
    	return this.quemanager;
    }
    private void QueueManagerObject(MQQueueManager v) {
    	this.quemanager = v;
    }
    
    private PCFMessageAgent messageAgent = null;
    private PCFMessageAgent MessageAgent() {
    	return this.messageAgent;
    }
    private void MessageAgent(PCFMessageAgent v) {
    	this.messageAgent = v;
    }

    private long numberOfMessagesProcessed;
    public long NumberOfMessagesProcessed() {
    	return this.numberOfMessagesProcessed;
    }
    public void IncrementNumberOfMessagesProcessed(long v) {
    	this.numberOfMessagesProcessed = v;
    }
    public void incrementNumberOfMessagesProcessed() {
    	this.numberOfMessagesProcessed++;
    }
    
    private int reasoncode;
    private void ReasonCode(int v) {
    	this.reasoncode = v;
    }
    public int ReasonCode() {
    	return this.reasoncode;
    }
	
	@Autowired
	private MQQueueManagerStats qmgrstats;
	private MQQueueManagerStats QMStatsObject() {
		return this.qmgrstats;
	}
    
    @Autowired
    public MQMetricsQueueManager mqmetricsqueuemanager;
    private MQMetricsQueueManager MQMetricQueueManager() {
    	return this.mqmetricsqueuemanager;
    }
    
	@Autowired
	private MQAccountingStats qmgraccountingstats;
	public MQAccountingStats AccountingStats() {
		return this.qmgraccountingstats;
	}
    
    @Bean
    public JSONController JSONController() {
    	return new JSONController();
    }
    
    // Constructor
	public MQConnection() {
	}
	
	@PostConstruct
	public void SetProperties() {
		
		log.info("MQConnection: Object created");
		try {
			log.info("OS : {}", System.getProperty("os.name").trim() );

		} catch (Exception e) {
			// continue
		}
		IncrementNumberOfMessagesProcessed(MQPCFConstants.PCF_INIT_VALUE);
		
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
    public void Scheduler() {
	
		try {
			if (MessageAgent() != null) {
				GetMetrics();
				
			} else {
				ConnectToQueueManager();
				
			}
			
		} catch (PCFException p) {
			log.error("PCFException " + p.getMessage());
			log.error("PCFException: ReasonCode " + p.getReason());
			ReasonCode(p.getReason());
			if (log.isTraceEnabled()) { p.printStackTrace(); }
			CloseQMConnection(p.getReason());
			QMStatsObject().ConnectionBroken(p.getReason());
			QueueManagerIsNotRunning(p.getReason());
			
		} catch (MQException m) {
			log.error("MQException " + m.getMessage());
			log.error("MQException: ReasonCode " + m.getReason());
			ReasonCode(m.getReason());
			if (m.getReason() == MQConstants.MQRC_UNSUPPORTED_CIPHER_SUITE) {
				log.error("Most likely cause is that the useIBMCipherMappings property is not set correctly");
			}
			if (log.isTraceEnabled()) { m.printStackTrace(); }
			CloseQMConnection(m.getReason());
			QMStatsObject().ConnectionBroken(m.getReason());
			QueueManagerIsNotRunning(m.getReason());
	    	MessageAgent(null);

		} catch (MQExceptionWrapper w) {
			log.error("MQException " + w.getMessage());
			log.error("MQException: ReasonCode " + w.getReason());
			ReasonCode(w.getReason());
			if (w.getReason() == MQConstants.MQRC_UNSUPPORTED_CIPHER_SUITE) {
				log.error("Most likely cause is that the useIBMCipherMappings property is not set correctly");
			}
			if (log.isTraceEnabled()) { w.printStackTrace(); }
			CloseQMConnection(w.getReason());
			QMStatsObject().ConnectionBroken(w.getReason());
			QueueManagerIsNotRunning(w.getReason());
	    	MessageAgent(null);

		} catch (IOException i) {
			log.error("IOException " + i.getMessage());
			if (log.isTraceEnabled()) { i.printStackTrace(); }
			ReasonCode(MQPCFConstants.ERROR_IO_EXCEPTION);
			CloseQMConnection();
			QMStatsObject().ConnectionBroken();
			QueueManagerIsNotRunning(MQPCFConstants.PCF_INIT_VALUE);
			
		} catch (Exception e) {
			log.error("Exception " + e.getMessage());
			if (log.isTraceEnabled()) { e.printStackTrace(); }
			ReasonCode(MQPCFConstants.ERROR_EXCEPTION);
			CloseQMConnection();
			QMStatsObject().ConnectionBroken();
			QueueManagerIsNotRunning(MQPCFConstants.PCF_INIT_VALUE);
		}
    }    
	
	/*
	 * Connect to the queue manager
	 */
	private void ConnectToQueueManager() throws MQException, MQDataException, IOException {
		
		log.warn("No MQ queue manager object");
		CreateQueueManagerConnection();
		QMStatsObject().ConnectionBroken();
		
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
	public void CreateQueueManagerConnection() throws MQException, MQDataException, IOException {
		
		WhichAuthentication();

		MQMetricQueueManager().CreateQueueManagerObject();
		QueueManagerObject(MQMetricQueueManager().QmgrManagerObject());
		MessageAgent(MQMetricQueueManager().CreateMessageAgent(QueueManagerObject()));
		
		MQMetricQueueManager().QueueManagerName(QueueManagerName());
		QMStatsObject().QueueManagerName(QueueManagerName());
		AccountingStats().QueueManagerName(QueueManagerName());

		if (RunningLocal()) {
			QMStatsObject().RunMode(MQPCFConstants.MODE_LOCAL);
			
		} else {
			QMStatsObject().RunMode(MQPCFConstants.MODE_CLIENT);
			
		}
		QMStatsObject().Version();
		MQMetricQueueManager().CalculateStartEndDates();
		
		MQMetricQueueManager().Queue(null);		
		MQMetricQueueManager().OpenQueueForReading();

	}
	
	/*
	 * Get the queue manager statistics value
	 */
	public int GetQueueManagerStatistics() {
		
		int ret = MQMetricQueueManager().getQueueManagerStatistics();
		return ret;
	}

	/*
	 * Get the queue manager accounting value
	 */
	public int GetQueueManagerAccounting() {
		
		int ret = MQMetricQueueManager().getQueueManagerAccounting();
		return ret;
	}
	
	/*
	 * Check authentication method ... user or certificates
	 */
	private void WhichAuthentication() {
		
		if (UserId() != null && (!UserId().isEmpty())) {
			if (UseSSL()) {
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
	private void QueueManagerIsNotRunning(int status) {

		QMStatsObject().QueueManagerNotRunning(status);

	}

	/*
	 * Get metrics
	 */
	public void GetMetrics() throws PCFException, MQException, 
			IOException, MQDataException, ParseException {
		
		MQMetricQueueManager().GetQueueManagerMonitoringValues();		
		QMStatsObject().QueueManagerStatus(MQMetricQueueManager().QueueManagerStatus());

		ProcessAccountingMetrics();
		
	}

	/*
	 * Get a list of AccountityEntity objects;
	 * Loop through each returned object
	 */
	private void ProcessAccountingMetrics() throws MQDataException, IOException, ParseException, MQException {

		List<AccountingEntity> list = MQMetricQueueManager().ReadAccountingData();		
		log.debug("AccountList count {}", list.size());
		if (!list.isEmpty()) {
			for (AccountingEntity ae : list) {
				AccountingStats().CreateMetric(ae);				
				incrementNumberOfMessagesProcessed();
			}
		}
	}
		
	/*
	 * Disconnect cleanly from the queue manager
	 */
    @PreDestroy
    public void Disconnect() {
    	CloseQMConnection();
    }
    
    /*
     * Disconnect, showing the reason
     */
    public void CloseQMConnection(int reasonCode) {

		log.info("Disconnected from the queue manager"); 
		log.info("Reason code: " + reasonCode);
		MQMetricQueueManager().CloseConnection(QueueManagerObject(), MessageAgent());
    	QueueManagerObject(null);
    	MessageAgent(null);
		
    }
	        
    public void CloseQMConnection() {

		log.info("Disconnected from the queue manager"); 
		MQMetricQueueManager().CloseConnection(QueueManagerObject(), MessageAgent());
		QueueManagerObject(null);
    	MessageAgent(null);

    }
}


