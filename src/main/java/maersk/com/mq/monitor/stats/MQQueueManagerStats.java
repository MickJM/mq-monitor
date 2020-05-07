package maersk.com.mq.monitor.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.ibm.mq.constants.MQConstants;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import maersk.com.mq.monitor.mqmetrics.MQMonitorBase;
import maersk.com.mq.monitor.mqmetrics.MQPCFConstants;

@Component
public class MQQueueManagerStats {

	@Autowired
	private MQMonitorBase base;

	@Value("${info.app.version:notset}")
	private String versionNumeric;
	public String getVersion() {
		return this.versionNumeric;
	}

	private String queueManagerName;
	public String getQueueManagerName() {
		return this.queueManagerName;
	}
	public void setQueueManagerName(String v) {
		this.queueManagerName = v;
	}
	
	private boolean connectionBroken;

	/*
     *  MAP details for the metrics
     */
    private Map<String,AtomicInteger>runModeMap = new HashMap<String,AtomicInteger>();
    private Map<String,AtomicInteger>versionMap = new HashMap<String,AtomicInteger>();
    private Map<String,AtomicInteger>queueManagerStatusMap = new HashMap<String,AtomicInteger>();

    protected static final String runMode = "mq:runMode";
    protected static final String version = "mq:monitoringVersion";
    protected static final String queueManagerStatus = "mq:queueManagerStatus";

    /*
     * Run Mode
     */
	public void setRunMode(int mode) {

		runModeMap.put(runMode, base.meterRegistry.gauge(runMode, 
				Tags.of("queueManagerName", getQueueManagerName()),
				new AtomicInteger(mode)));
	}
	
	/*
	 * API Version
	 */
	public void setVersion() {

		String appVersion = getVersion();
		String s = appVersion.replaceAll("[\\s.]", "");
		int v = Integer.parseInt(s);		
		versionMap.put(version, base.meterRegistry.gauge(version, 
				new AtomicInteger(v)));		
	}
	
    /*
     * Queue manager is not running, so set the status
     */
	public void QueueManagerNotRunning(int status) {
		
		int val = MQPCFConstants.PCF_INIT_VALUE;		
		if (status == MQConstants.MQRC_STANDBY_Q_MGR) {
			val = MQConstants.MQQMSTA_STANDBY;
		} 		
		if (this.connectionBroken) {
			if (status == MQConstants.MQRC_Q_MGR_QUIESCING) {
				val = MQConstants.MQQMSTA_QUIESCING;
			} 
			if (status == MQConstants.MQRC_CONNECTION_QUIESCING) {
				val = MQConstants.MQQMSTA_QUIESCING;
			} 

			//if (status == MQConstants.MQRC_JSSE_ERROR) {
			//	val = MQConstants.MQQMSTA_QUIESCING;
			//} 
			if (status == MQConstants.MQRC_CONNECTION_BROKEN) {
				val = MQConstants.MQQMSTA_QUIESCING;
			} 

		}
	
		queueManagerStatus(val);
	}
	
	public void queueManagerStatus(int v) {
		
		AtomicInteger value = queueManagerStatusMap.get(queueManagerStatus + "_" + getQueueManagerName());
		if (value == null) {
			queueManagerStatusMap.put(queueManagerStatus + "_" + getQueueManagerName(),
					base.meterRegistry.gauge(queueManagerStatus, 
					Tags.of("queueManagerName", getQueueManagerName()
							),
					new AtomicInteger(v))
					);
		} else {
			value.set(v);
		}		
	}
	
	/*
	 * Connection Broken ?
	 */
	public void connectionBroken(int status) {
		if (status == MQConstants.MQRC_CONNECTION_BROKEN
				|| status == MQConstants.MQRC_CONNECTION_QUIESCING
				|| status == MQConstants.MQRC_Q_MGR_QUIESCING) {
			this.connectionBroken = true;
		} else {
			connectionBroken();
		}
		
	}
	public void connectionBroken() {
		this.connectionBroken = false;
	}

}
