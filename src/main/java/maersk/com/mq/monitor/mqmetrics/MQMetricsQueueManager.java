package maersk.com.mq.monitor.mqmetrics;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.MQCFGR;
import com.ibm.mq.headers.pcf.MQCFIL;
import com.ibm.mq.headers.pcf.MQCFIN;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFMessageAgent;
import com.ibm.mq.headers.pcf.PCFParameter;
import maersk.com.mq.monitor.accounting.AccountingEntity;

@Component
public class MQMetricsQueueManager<T> {

	private final static Logger log = LoggerFactory.getLogger(MQMetricsQueueManager.class);	
				
	private boolean onceOnly = true;
	public void setOnceOnly(boolean v) {
		this.onceOnly = v;
	}
	public boolean getOnceOnly() {
		return this.onceOnly;
	}
	
	// taken from connName
	private String hostName;
	public void setHostName(String v) {
		this.hostName = v;
	}
	public String getHostName() { return this.hostName; }
	
	@Value("${ibm.mq.queueManager}")
	private String queueManager;
	public void setQueueManager(String v) {
		this.queueManager = v;
	}
	public String getQueueManagerName() { return this.queueManager; }
	
	// hostname(port)
	@Value("${ibm.mq.connName:#{null}}")
	private String connName;
	public void setConnName(String v) {
		this.connName = v;
	}
	public String getConnName() { return this.connName; }
	
	@Value("${ibm.mq.channel:#{null}}")
	private String channel;
	public void setChannelName(String v) {
		this.channel = v;
	}
	public String getChannelName() { return this.channel; }

	// taken from connName
	private int port;
	public void setPort(int v) {
		this.port = v;
	}
	public int getPort() { return this.port; }

	@Value("${ibm.mq.user:#{null}}")
	private String userId;
	public void setUserId(String v) {
		this.userId = v;
	}
	public String getUserId() { return this.userId; }

	@Value("${ibm.mq.password:#{null}}")
	private String password;
	public void setPassword(String v) {
		this.password = v;
	}
	public String getPassword() { return this.password; }
	
	@Value("${ibm.mq.sslCipherSpec}")
	private String cipher;
	
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
	
	@Value("${ibm.mq.multiInstance:false}")
	private boolean multiInstance;
	public boolean isMultiInstance() {
		return this.multiInstance;
	}
	
	@Value("${ibm.mq.local:false}")
	private boolean local;
	public boolean isRunningLocal() {
		return this.local;
	}

	@Value("${ibm.mq.ccdtFile:#{null}}")
	private String ccdtFile;
	public String getCCDTFile() {
		return this.ccdtFile;
	}
	
	@Value("${ibm.mq.objects.queues.exclude}")
    private String[] excludeQueues;
	@Value("${ibm.mq.objects.queues.include}")
    private String[] includeQueues;
	
	@Value("${ibm.mq.pcf.accountingType:MQCFT_STATISTICS}")
	private String accountingType;
	private String getAccountingType() {
		return this.accountingType;
	}
	private void setAccountingType(String v) {
		this.accountingType = v;
	}

	private int searchAccountingType;
	public void setSearchAccountingType(int v) {
		this.searchAccountingType = v;
	}
	public int getSearchAccountingType() {
		return this.searchAccountingType;
	}

	@Value("${ibm.mq.pcf.parameters:#{null}}")
	private String[] pcfParameters;
	private String[] getPCFParameters() {
		return this.pcfParameters;
	}
	private int[] searchPCF;
	public void setSearchPCF(int[] v) {
		this.searchPCF = v;
	}
	public int[] getSearchPCF() {
		return this.searchPCF;
	}
	
	@Value("${ibm.mq.pcf.browse:false}")
	private boolean pcfBrowse;	
	public boolean getBrowse() {
		return this.pcfBrowse;
	}

	@Value("${ibm.mq.pcf.period.start:#{null}}")
	private String startDate;	
	public String getStartDate() {
		return this.startDate;
	}
	public void setStartDate(String v) {
		this.startDate = v;
	}
	
	private Date startDateObject;
	public Date getStartDateObject() {
		return this.startDateObject;
	}
	public void setStartDateObject(Date v) {
		this.startDateObject = v;
	}
	
	@Value("${ibm.mq.pcf.period.end:#{null}}")
	private String endDate;	
	public String getEndDate() {
		return this.endDate;
	}
	public void setEndDate(String v) {
		this.endDate = v;
	}

	private Date endDateObject;
	public Date getEndDateObject() {
		return this.endDateObject;
	}
	public void setEndDateObject(Date v) {
		this.endDateObject = v;
	}
	
	@Value("${info.app.name:MQMonitor}")
	private String appName;	
	public String getAppName() {
		return this.appName;
	}
	
	private int statType;
	public void setStatType(int v) {
		this.statType = v;
	}
	public int getStatType() {
		return this.statType;
	}

	
	/*
	 * Accounting saved value
	 */
	private int savedQAcct;
    public int getSavedQAcct() {
		return savedQAcct;
    }
	public void setSavedQAcct(int value) {
		this.savedQAcct = value;
	}
	/*
	 * Statistics saved value
	 */
	private int savedQStat;
    public int getSavedQStat() {
		return savedQStat;
    }
	public void setSavedQStat(int value) {
		this.savedQStat = value;
	}
	private boolean qAcct;
	public void setQAcct(boolean v) {
		this.qAcct = v;
	}
	public boolean getQAcct() {
		return qAcct;
	}
	
	/*
	 * Validate connection name and userID
	 */
	private boolean validConnectionName() {
		return (getConnName().equals(""));
	}
	private boolean validateUserId() {
		return (getUserId().equals(""));		
	}
	private boolean validateUserId(String v) {
		boolean ret = false;
		if (getUserId().equals(v)) {
			ret = true;
		}
		return ret;
	}
	
	private PCFMessageAgent messageAgent;
    public void setMessageAgent(PCFMessageAgent v) {
    	this.messageAgent = v;
    }
    public PCFMessageAgent getMessageAgent() {
    	return this.messageAgent;
    }
	
    private MQQueueManager queManager;
    public void setQmgr(MQQueueManager qm) {
    	this.queManager = qm;
    }
    public MQQueueManager getQmgr() {
    	return this.queManager;
    }

    private MQQueue queue = null;
    public void setQueue(MQQueue q) {
    	this.queue = q;
    }
    public MQQueue getQueue() {
    	return this.queue;
    }
    
    private MQGetMessageOptions gmo = null;
    public void setGMO(MQGetMessageOptions gmo) {
    	this.gmo = gmo;
    }
    public MQGetMessageOptions getGMO() {
    	return this.gmo;
    }
    
    /*
     * Queue Manager Accounting setting
     */
	private int qmgrAccounting;
	public synchronized void setAccounting(int v) {		
		this.qmgrAccounting = v;
	}
	public synchronized int getAccounting() {		
		return this.qmgrAccounting;
	}
	/*
	 * Queue Manager Statistics setting
	 */
	private int qmgrStats;
	public synchronized void setQueueManagerStatistics(int v) {
		this.qmgrStats = v;
	}
	public synchronized int getQueueManagerStatistics() {
		return this.qmgrStats;
	}
	/*
	 * Queue Manager Statistics Status
	 */
	private int qmgrStatus;
	public synchronized void setQueueManagerStatus(int v) {
		this.qmgrStatus = v;
	}
	public synchronized int getQueueManagerStatus() {
		return this.qmgrStatus;
	}
	
	protected final String LOW = "1970-01-01 00.00.00";
	protected final String HIGH = "9999-12-31 23.59.59";

	@Autowired
    private MQMonitorBase base;

    /*
     * Constructor
     */
	public MQMetricsQueueManager() {
	}
	
	@PostConstruct
	public void init() throws Exception {
		
		if (getPCFParameters() != null) {
			setSearchPCF(new int[getPCFParameters().length]);
			int[] s = new int[getPCFParameters().length];
			int array = 0;
		
			for (String w: getPCFParameters()) {
				final int x = MQConstants.getIntValue(w);
				if ((x != MQConstants.MQIAMO_PUT_MAX_BYTES) && (x != MQConstants.MQIAMO_GET_MAX_BYTES)
						&& (x != MQConstants.MQIAMO_PUTS) && (x != MQConstants.MQIAMO_GETS)
						&& (x != MQConstants.MQIAMO_PUTS_FAILED) && (x != MQConstants.MQIAMO_GETS_FAILED)) {
					log.error("Invalid PCF parameter : " + MQConstants.lookup(x, null));
					throw new Exception("Invalid PCF Parameter");
				}
				s[array] = x;
				array++;		
			}
			setSearchPCF(s);
			Arrays.sort(getSearchPCF());	
			
		} else {
			log.info("No accounting metrics specified to be collected");
			throw new Exception("No accounting metrics specified to be collected");

		}

		if (getAccountingType() != null) {
			int x = MQConstants.getIntValue(getAccountingType());
			if ((x != MQConstants.MQCFT_ACCOUNTING) && (x != MQConstants.MQCFT_STATISTICS)) {
				log.warn("ibm.mq.pcf.accountingType is not set correctly, using MQCFT_ACCOUNTING");
				setAccountingType("MQCFT_ACCOUNTING");
				x = MQConstants.getIntValue(getAccountingType());
			}
			setSearchAccountingType(x);

			if (MQConstants.getIntValue(getAccountingType()) == MQConstants.MQCFT_STATISTICS) {
				if ((Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_PUT_MAX_BYTES) >= 0) 
						|| (Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_GET_MAX_BYTES) >= 0)) {
					log.warn("Statistics and PUT MAX / GET MAX are multually exclusive ");
					log.warn("Set ibm.mq.pcf.accountingType to MQCFT_ACCOUNTING to collect MAX PUT or MAX GET values ");
				}
			}
		} 	
		
		setQAcct(true);
	}
		
	/*
	 * Create an MQQueueManager object
	 */
	@SuppressWarnings("rawtypes")
	public MQQueueManager createQueueManager() throws MQException, MQDataException, MalformedURLException {
		
		Hashtable<String, Comparable> env = new Hashtable<String, Comparable>();
		
		if (!isRunningLocal()) { 
			
			getEnvironmentVariables();
			log.info("Attempting to connect using a client connection"); 
			if ((getCCDTFile() == null) || (getCCDTFile().isEmpty())) {
				env.put(MQConstants.HOST_NAME_PROPERTY, getHostName());
				env.put(MQConstants.CHANNEL_PROPERTY, getChannelName());
				env.put(MQConstants.PORT_PROPERTY, getPort());
			}
			
			/*
			 * 
			 * If a username and password is provided, then use it
			 * ... if CHCKCLNT is set to OPTIONAL or RECDADM
			 * ... RECDADM will use the username and password if provided ... if a password is not provided
			 * ...... then the connection is used like OPTIONAL
			 */		
		
			if (!StringUtils.isEmpty(getUserId())) {
				env.put(MQConstants.USER_ID_PROPERTY, getUserId()); 
			}
			if (!StringUtils.isEmpty(this.password)) {
				env.put(MQConstants.PASSWORD_PROPERTY, getPassword());
			}
			env.put(MQConstants.TRANSPORT_PROPERTY,MQConstants.TRANSPORT_MQSERIES);
			env.put(MQConstants.APPNAME_PROPERTY,getAppName());
			
			if (isMultiInstance()) {
				if (getOnceOnly()) {
					log.info("MQ Metrics is running in multiInstance mode");
					setOnceOnly(false);
				}
			}
			
			log.debug("Host		: " + getHostName());
			log.debug("Channel	: " + getChannelName());
			log.debug("Port		: " + getPort());
			log.debug("Queue Man	: " + getQueueManagerName());
			log.debug("User		: " + getUserId());
			log.debug("Password	: **********");
			if (usingSSL()) {
				log.debug("SSL is enabled ....");
			}
			
			// If SSL is enabled (default)
			if (usingSSL()) {
				if (!StringUtils.isEmpty(this.truststore)) {
					System.setProperty("javax.net.ssl.trustStore", this.truststore);
			        System.setProperty("javax.net.ssl.trustStorePassword", this.truststorepass);
			        System.setProperty("javax.net.ssl.trustStoreType","JKS");
			        System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings","false");
				}
				if (!StringUtils.isEmpty(this.keystore)) {
			        System.setProperty("javax.net.ssl.keyStore", this.keystore);
			        System.setProperty("javax.net.ssl.keyStorePassword", this.keystorepass);
			        System.setProperty("javax.net.ssl.keyStoreType","JKS");
				}
				if (!StringUtils.isEmpty(this.cipher)) {
					env.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, this.cipher);
				}
			
			} else {
				log.debug("SSL is NOT enabled ....");
			}
			
	        //System.setProperty("javax.net.debug","all");
			if (!StringUtils.isEmpty(this.truststore)) {
				log.debug("TrustStore       : " + this.truststore);
				log.debug("TrustStore Pass  : ********");
			}
			if (!StringUtils.isEmpty(this.keystore)) {
				log.debug("KeyStore         : " + this.keystore);
				log.debug("KeyStore Pass    : ********");
				log.debug("Cipher Suite     : " + this.cipher);
			}
		} else {
			log.info("Attempting to connect using a local bindings"); 
		}
		
		/*
		 * Connect to the queue manager 
		 * ... local connection : application connection in local bindings
		 * ... client connection: application connection in client mode 
		 */
		MQQueueManager qmgr = null;
		if (isRunningLocal()) {
			log.info("Attemping to connect to queue manager {} using local bindings", getQueueManagerName());
			qmgr = new MQQueueManager(getQueueManagerName());
			
		} else {
			if ((getCCDTFile() == null) || (getCCDTFile().isEmpty())) {
				log.info("Attempting to connect to queue manager {} ", getQueueManagerName());
				qmgr = new MQQueueManager(getQueueManagerName(), env);
				
			} else {
				URL ccdtFileName = new URL("file:///" + getCCDTFile());
				log.info("Attempting to connect to queue manager {} using CCDT file", getQueueManagerName());
				qmgr = new MQQueueManager(this.queueManager, env, ccdtFileName);
				
			}
		}
		log.info("Connection to queue manager established ");
		setQmgr(qmgr);
		
		return qmgr;
	}
	
	/*
	 * Create a PCF agent
	 */	
	public PCFMessageAgent createMessageAgent(MQQueueManager queManager) throws MQDataException {
		
		log.info("Attempting to create a PCFAgent ");
		PCFMessageAgent pcfmsgagent = new PCFMessageAgent(queManager);
		log.info("PCFAgent created successfully");
		
		setMessageAgent(pcfmsgagent);
		
		return pcfmsgagent;	
		
	}
	
	/*
	 * Get MQ details from environment variables
	 */
	private void getEnvironmentVariables() {
		
		if (getConnName() == null) {
			return;
		}
		
		/*
		 * ALL parameter are passed in the application.yaml file ...
		 *    These values can be overrrided using an application-???.yaml file per environment
		 *    ... or passed in on the command line
		 */
		
		// Split the host and port number from the connName ... host(port)
		if (!validConnectionName()) {
			Pattern pattern = Pattern.compile("^([^()]*)\\(([^()]*)\\)(.*)$");
			Matcher matcher = pattern.matcher(this.connName);	
			if (matcher.matches()) {
				this.hostName = matcher.group(1).trim();
				this.port = Integer.parseInt(matcher.group(2).trim());
			
			} else {
				log.error("While attempting to connect to a queue manager, the connName is invalid ");
				System.exit(MQPCFConstants.EXIT_ERROR);				
			
			}
			
		} else {
			log.info("While attempting to connect to a queue manager, the connName is missing  ");
			
		}

		// if no user, forget it ...
		if (getUserId() == null) {
			return;
		}
		
		/*
		 * If we dont have a user or a certs are not being used, then we cant connect ... unless we are in local bindings
		 */
		if (validateUserId()) {
			if (!usingSSL()) {
				log.error("Unable to connect to queue manager, credentials are missing and certificates are not being used");
				System.exit(MQPCFConstants.EXIT_ERROR);
			}
		}

		/*
		 * dont allow mqm user
		 */
		if (!validateUserId()) {
			if ((validateUserId("mqm") || (validateUserId("MQM")))) {
				log.error("The MQ channel USERID must not be running as 'mqm' ");
				System.exit(MQPCFConstants.EXIT_ERROR);
			}
		} else {
			this.userId = null;
			this.password = null;
		}
	
	}
		
	/*
	 * Get the Accounting / Stats details from the queue manager
	 */
	public void getQueueManagerMonitoringValues() throws MQDataException, IOException {
		
		/*
		 *  Inquire on the queue manager ...
		 */
		int[] pcfParmAttrs = { MQConstants.MQIACF_ALL };
		PCFMessage pcfRequest = new PCFMessage(MQConstants.MQCMD_INQUIRE_Q_MGR);
		pcfRequest.addParameter(MQConstants.MQIACF_Q_MGR_ATTRS, pcfParmAttrs);
		PCFMessage[] pcfResponse = getMessageAgent().send(pcfRequest);		
		PCFMessage response = pcfResponse[0];
	
		/*
		 *  Save the queue monitoring attribute to be used later
		 */
		int queueMon = response.getIntParameterValue(MQConstants.MQIA_MONITORING_Q);

		/*
		 *  Save the statistics status
		 */
		int stats = response.getIntParameterValue(MQConstants.MQIA_STATISTICS_Q);
		//setQueueManagerStatistics(stats);
		if (getSavedQStat() != response.getIntParameterValue(MQConstants.MQIA_STATISTICS_Q)) {
			setQueueManagerStatistics(stats);
			setSavedQStat(stats);
			setQAcct(true);			
		}
		
		/*
		 *  Save the accounting status
		 */
		int qAcctValue = response.getIntParameterValue(MQConstants.MQIA_ACCOUNTING_Q);
		if (getSavedQAcct() != response.getIntParameterValue(MQConstants.MQIA_ACCOUNTING_Q)) {
			setAccounting(qAcctValue);
			setSavedQAcct(qAcctValue);
			setQAcct(true);
		}

		if (getQAcct()) {
			String s = getAccountingStatus(qAcctValue);
			log.info("Queue manager accounting is set to " + s);
			s = getAccountingStatus(stats);			
			log.info("Queue manager statistics is set to " + s);			
			setQAcct(false);
		}
		
		/*
		 *  Send a queue manager status request
		 */
		pcfRequest = new PCFMessage(MQConstants.MQCMD_INQUIRE_Q_MGR_STATUS);
		pcfRequest.addParameter(MQConstants.MQIACF_Q_MGR_STATUS_ATTRS, pcfParmAttrs);
		pcfResponse = getMessageAgent().send(pcfRequest);		
		response = pcfResponse[0];       	
		
		int qmStatus = response.getIntParameterValue(MQConstants.MQIACF_Q_MGR_STATUS);
		setQueueManagerStatus(qmStatus);
		
	}
	
	/*
	 * Open the queue for reading ...
	 */
	public void openQueueForReading() throws MQException {

		if (getQueue() == null) {
			int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF |
					MQConstants.MQOO_BROWSE |
					MQConstants.MQOO_FAIL_IF_QUIESCING;			

			if (getSearchAccountingType() == MQConstants.MQCFT_ACCOUNTING) {
				setQueue(getQmgr().accessQueue("SYSTEM.ADMIN.ACCOUNTING.QUEUE", openOptions));
				setStatType(MQConstants.MQCFT_ACCOUNTING);
			}
			if (getSearchAccountingType() == MQConstants.MQCFT_STATISTICS) {
				setQueue(getQmgr().accessQueue("SYSTEM.ADMIN.STATISTICS.QUEUE", openOptions));
				setStatType(MQConstants.MQCFT_STATISTICS);
			}
			setGMO(new MQGetMessageOptions());

		}
		
		int gmoptions = MQConstants.MQGMO_NO_WAIT |
				MQConstants.MQGMO_CONVERT;
		if (getBrowse()) {
			gmoptions = MQConstants.MQGMO_BROWSE_FIRST;

		} 
		getGMO().options = gmoptions;
		getGMO().matchOptions = MQConstants.MQMO_MATCH_MSG_ID  | MQConstants.MQMO_MATCH_CORREL_ID;
		
	}
	
	/*
	 * Calculate pcf period dates
	 */
	public void calculateStartEndDates() {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
		Date high = null;
		Date low = null;
		
		try {
			low = dateFormat.parse(LOW);
			setStartDate(LOW);
			Date d1 = dateFormat.parse(getStartDate());
			setStartDateObject(d1);
			
		} catch (NullPointerException | ParseException e) {
			log.warn("Start Date time period is invalid: format must be yyyy-MM-dd HH.mm.ss");
			log.warn("Start Date time period will not be used");
			setStartDateObject(low);
		}		
		
		try {
			high = dateFormat.parse(HIGH);
			setEndDate(HIGH);
			Date d2 = dateFormat.parse(getEndDate());
			setEndDateObject(d2);

		} catch (NullPointerException | ParseException e) {
			log.warn("End Date time period is invalid: format must be yyyy-MM-dd HH.mm.ss");
			log.warn("End Date time period will not be used");
			setEndDateObject(high);
		}		
		
		if ((getStartDateObject() != null) || (getStartDateObject() != null)) {
			log.info("Accounting / Statistics period is; start: " + getStartDate() + " end: " + getEndDate());

		} else {
			log.info("Accounting / Statistics period is set for dates / times");
			
		}

		log.info("accountingType is set as; " + getAccountingType());
		
	}

	/*
	 * Look for the account information for MAX message size on a queue ...
	 * This is complex ...
	 * 
	 * Open the 'SYSTEM.ADMIN.ACCOUNTING.QUEUE'
	 *    if 'browse', set to browse mode, if 'read' set to read messages
	 *    For each messages on the queue;
	 *        Read the message of the queue (PCF format)
	 *        Loop through each PCF parameter on the message
	 *            Found a PCF Group record (MQCFT_GROUP)
	 *                Loop through each PCF parameter within the group
	 *                    When parameter is MQCA_Q_NAME
	 *                        if not a queue we want; break
	 *                    When parameter is MQCA_Q_NAME
	 *                        save the queue name
	 *                    When parameter is MQIAMO_PUTS
	 *                        create an AccountingEntity object
	 *                        break
	 *                    When parameter is MQIAMO_GETS
	 *                        create an AccountingEntity object
	 *                        break
	 *                    When parameter is MQIAMO_PUT_MAX_BYTES
	 *                        create an AccountingEntity object
	 *                        break
	 *                    When parameter is MQIAMO_GET_MAX_BYTES
	 *                        create an AccountingEntity object
	 *                        break
	 *                        
	 *                :
	 *            :
	 *        :
	 *        
	 */
	public List<AccountingEntity> readAccountData() throws MQDataException, IOException, MQException {

		final List<AccountingEntity> stats = new ArrayList<AccountingEntity>();
		stats.clear();
		
		// https://github.com/icpchave/MQToolsBox/blob/master/src/cl/continuum/mq/pfc/samples/ReadPCFMessages.java
		if (getSearchPCF().length == 0) {
			return stats;
		}
		
		/*
		 * Process the messages that we have ...
		 */
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
		if (log.isTraceEnabled()) {
			Date date = new Date();
			log.trace("Start accounting processing : " + dateFormat.format(date));
		}
		
		try {
							
			String pcfQueueName = "";			
			MQMessage message = new MQMessage ();
			while (true) {
			
				message.messageId = MQConstants.MQMI_NONE;
				message.correlationId = MQConstants.MQMI_NONE;
				getQueue().get (message, getGMO());
				
				/*
				 * Only process ADMIN messages ...
				 */
				if (message.format.equals(MQConstants.MQFMT_ADMIN)) {
					PCFMessage pcf = new PCFMessage (message);
					/*
					 * Accounting or Stats ?
					 */
					if ((pcf.getCommand() == MQConstants.MQCMD_STATISTICS_Q) || (pcf.getCommand() == MQConstants.MQCMD_ACCOUNTING_Q)) {						

						int cont = pcf.getControl();
						Enumeration<PCFParameter> parms = pcf.getParameters();					
						String startDate = pcf.getStringParameterValue(MQConstants.MQCAMO_START_DATE).trim();
						String startTime = pcf.getStringParameterValue(MQConstants.MQCAMO_START_TIME).trim();
						Date d1 = null;
						Date d2 = null;
						try {
							d1 = dateFormat.parse(startDate + " " + startTime);
						
						} catch (ParseException e) {
							// carry on
						}

						String endDate = pcf.getStringParameterValue(MQConstants.MQCAMO_END_DATE).trim();
						String endTime = pcf.getStringParameterValue(MQConstants.MQCAMO_END_TIME).trim();
						try {
							d2 = dateFormat.parse(endDate + " " + endTime);
						
						} catch (ParseException e) {
							// carry on
						}
						if (getStartDateObject().before(d1) && getEndDateObject().after(d2)) {
							log.debug("Record found within date range ... control : " + cont);
							msgPCFRecords: while (parms.hasMoreElements()) {
								PCFParameter pcfParams = parms.nextElement();
							
								switch (pcfParams.getParameter()) {

									default:
										switch (pcfParams.getType()) {
											case(MQConstants.MQCFT_GROUP):
												MQCFGR grp = (MQCFGR)pcfParams; // PCF Group record
												Enumeration<PCFParameter> gparms = grp.getParameters();
												
												grpRecords: while (gparms.hasMoreElements()) {
													PCFParameter grpPCFParams = gparms.nextElement();
													
													switch (grpPCFParams.getParameter()) {
														case (MQConstants.MQCA_Q_NAME):
															pcfQueueName = grpPCFParams.getStringValue().trim();
															if (!checkQueueNames(pcfQueueName)) {
																log.debug("Filters excluded records for " + pcfQueueName);
																break grpRecords;
															}
															break; // get next group record

														case (MQConstants.MQIAMO_GETS):
															if (Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_GETS) >= 0) {
																MQCFIL max = (MQCFIL) grpPCFParams;
																final int[] pcfArrayValue = max.getValues();
																if ((pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] > 0) 
																		|| (pcfArrayValue[MQConstants.MQPER_PERSISTENT] > 0)) {

																	if (pcfQueueName != "") {																		
																		AccountingEntity ae = createEntity(MQConstants.MQIAMO_GETS, 
																				pcfQueueName, pcfArrayValue,
																				startDate, startTime, endDate, endTime);
																		stats.add(ae);																		
																		log.debug("GETS: " + pcfQueueName + " { " 
																					+ pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] + ", " + pcfArrayValue[MQConstants.MQPER_PERSISTENT] + " } ");
																	}														
																}
															}
															break;

														case (MQConstants.MQIAMO_PUTS):
															if (Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_PUTS) >= 0) {
																MQCFIL max = (MQCFIL) grpPCFParams;
																final int[] pcfArrayValue = max.getValues();
																if ((pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] > 0) 
																		|| (pcfArrayValue[MQConstants.MQPER_PERSISTENT] > 0)) {

																	if (pcfQueueName != "") {
																		AccountingEntity ae = createEntity(MQConstants.MQIAMO_PUTS, 
																				pcfQueueName, pcfArrayValue,
																				startDate, startTime, endDate, endTime);
																		stats.add(ae);
																		log.debug("PUTS: " + pcfQueueName + " { " 
																				+ pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] + ", " + pcfArrayValue[MQConstants.MQPER_PERSISTENT] + " } ");

																	}														
																}
															}
															break;
														
														case (MQConstants.MQIAMO_PUT_MAX_BYTES):
															if (Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_PUT_MAX_BYTES) >= 0) {
																MQCFIL max = (MQCFIL) grpPCFParams;
																final int[] pcfArrayValue = max.getValues();
																if ((pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] > 0) 
																		|| (pcfArrayValue[MQConstants.MQPER_PERSISTENT] > 0)) {

																	if (pcfQueueName != "") {

																		AccountingEntity ae = createEntity(MQConstants.MQIAMO_PUT_MAX_BYTES, pcfQueueName, pcfArrayValue,
																				startDate, startTime, endDate, endTime);
																		stats.add(ae);
																		log.debug("PUTS MAX BYTES: " + pcfQueueName + " { " 
																				+ pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] + ", " + pcfArrayValue[MQConstants.MQPER_PERSISTENT] + " } ");

																	}
																}
															}											
															break;

														case (MQConstants.MQIAMO_GET_MAX_BYTES):
															if (Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_GET_MAX_BYTES) >= 0) {							
																MQCFIL max = (MQCFIL) grpPCFParams;
																final int[] pcfArrayValue = max.getValues();
																if ((pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] > 0) 
																		|| (pcfArrayValue[MQConstants.MQPER_PERSISTENT] > 0)) {

																	if (pcfQueueName != "") {
																		AccountingEntity ae = createEntity(MQConstants.MQIAMO_GET_MAX_BYTES, 
																				pcfQueueName, pcfArrayValue,
																				startDate, startTime, endDate, endTime);
																		stats.add(ae);
																		log.debug("GETS MAX BYTES: " + pcfQueueName + " { " 
																			+ pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] + ", " + pcfArrayValue[MQConstants.MQPER_PERSISTENT] + " } ");
																	}
																}
																break;  // break from the loop, as we dont want to continue processing any more
														    			 // if we want 'browse stats', move the 'break msgPCFRecords' to the end
															}										
															break;

															
														case (MQConstants.MQIAMO_GETS_FAILED):
															if (Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_GETS_FAILED) >= 0) {							
																MQCFIN max = (MQCFIN) grpPCFParams;
																final int[] pcfArrayValue = {max.getIntValue(),0};
																if (pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
																	
																	if (pcfQueueName != "") {
																		AccountingEntity ae = createEntity(MQConstants.MQIAMO_GETS_FAILED, 
																				pcfQueueName, pcfArrayValue,
																				startDate, startTime, endDate, endTime);
																		stats.add(ae);
																		log.debug("GETS FAILS: " + pcfQueueName + " { " 
																			+ pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] + ", " + pcfArrayValue[MQConstants.MQPER_PERSISTENT] + " } ");
																	}
																}
																break;  // break from the loop, as we dont want to continue processing any more
																					 // if we want 'browse stats', move the 'break msgPCFRecords' to the end
															}										
															break;
															
														case (MQConstants.MQIAMO_PUTS_FAILED):
															if (Arrays.binarySearch(getSearchPCF(), MQConstants.MQIAMO_PUTS_FAILED) >= 0) {							
																MQCFIN max = (MQCFIN) grpPCFParams;
																final int[] pcfArrayValue = {max.getIntValue(),0};
																if (pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] > 0) {
	
																	if (pcfQueueName != "") {
																		AccountingEntity ae = createEntity(MQConstants.MQIAMO_PUTS_FAILED, 
																				pcfQueueName, pcfArrayValue,
																				startDate, startTime, endDate, endTime);
																		stats.add(ae);
																		log.debug("PUTS FAILS: " + pcfQueueName + " { " 
																			+ pcfArrayValue[MQConstants.MQPER_NOT_PERSISTENT] + ", " + pcfArrayValue[MQConstants.MQPER_PERSISTENT] + " } ");
																	}
																}
																break;	 // msgPCFRecords; break from the loop, as we dont want to continue processing any more
																					 // if we want 'browse stats', move the 'break msgPCFRecords' to the end
															}										
															break;
															
														default:
															break;
															
													} // end of switch
													
												} // end of loop
										        break;
										}
										break;
								}
							} // end of inner loop		        
						}
					} 	
					deleteMessagesUnderCursor();
					
					/*
					 * Reset options incase we deleted the message in the deleteMessageUnderCursor method
					 */
					getGMO().options = MQConstants.MQGMO_BROWSE_NEXT | 
							MQConstants.MQGMO_NO_WAIT | 
							MQConstants.MQGMO_CONVERT;
				
				} // end of ADMIN messages

			} // end of loop

		} catch (MQException e) {
			if (e.getReason() != MQConstants.MQRC_NO_MSG_AVAILABLE) {
				throw new MQException(e.getCompCode(), e.getReason(), e);
			}
		} 

		if (log.isTraceEnabled()) {
			Date date = new Date();
			log.trace("End accounting time   : " + dateFormat.format(date));
		}
		
		return stats;
	}
		
	/*
	 * Create accounting entity
	 */
	private AccountingEntity createEntity(int pcfType, String pcfQueueName, int[] pcfArrayValue,
			String startDate, String startTime, String endDate, String endTime  ) {
		
		AccountingEntity ae = new AccountingEntity();
		ae.setMonitoringType(getStatType());
		ae.setType(pcfType);
		ae.setQueueManagerName(getQueueManagerName());
		ae.setQueueName(pcfQueueName);
		ae.setValues(pcfArrayValue);
		ae.setStartDate(startDate);
		ae.setStartTime(startTime);
		ae.setEndDate(endDate);
		ae.setEndTime(endTime);
		
		return ae;
	}
	
	/*
	 * Delete message
	 */
	private void deleteMessagesUnderCursor() {

		/*
		 * For the queue we are looking for ...
		 *    if we want to, remove the message from the accounting queue
		 */
		if (!getBrowse()) {
			MQMessage message = new MQMessage ();		
			getGMO().options = MQConstants.MQGMO_MSG_UNDER_CURSOR | 
					MQConstants.MQGMO_NO_WAIT | 
					MQConstants.MQGMO_CONVERT;
			try {
				getQueue().get (message, getGMO());
				log.info("Deleting message ...." );

			} catch (Exception e) {
				/*
				 * If we fail, then someone else might has removed
				 * the message, so continue
				 */
			}
		}
		
	}
	
	/*
	 * Check for the queue names
	 */
	private boolean checkQueueNames(String name) {

		if (name.equals(null)) {
			return false;
		}
		
		// Exclude ...
		for (String s : this.excludeQueues) {
			if (s.equals("*")) {
				break;
			} else {
				if (name.startsWith(s)) {
					return false;
				}
			}
		}
	
		// Check queues against the list 
		for (String s : this.includeQueues) {
			if (s.equals("*")) {
				return true;
			} else {
				if (name.startsWith(s)) {
					return true;
				}				
			}
		}		
		return false;
	}
	
	/*
	 * What Is the accounting type set to ?
	 */
	private String getAccountingStatus(int v) {
		String s = "";
		switch (v) {
			case MQConstants.MQMON_NONE:
				s = "NONE";
				break;
			case MQConstants.MQMON_OFF:
				s = "OFF";
				break;
			case MQConstants.MQMON_ON:
				s = "ON";
				break;
			default:
				s = "OFF";
				break;	
		}
		return s;
	}
	
	/*
	 * Close the connection to the queue manager
	 */
	public void CloseConnection(MQQueueManager qm, PCFMessageAgent ma) {
		
    	try {
    		if (qm.isConnected()) {
	    		log.debug("Closing MQ Connection ");
    			qm.disconnect();
    		}
    	} catch (Exception e) {
    		// do nothing
    	}
    	
    	try {
	    	if (ma != null) {
	    		log.debug("Closing PCF agent ");
	        	ma.disconnect();
	    	}
    	} catch (Exception e) {
    		// do nothing
    	}
	}
	
	
}
