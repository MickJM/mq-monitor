package app.com.mq.monitor.accounting;

/*
 * Accounting PCF entity
 */
public class AccountingEntity {

	private int monitoringType;
	private int pcfType;
	private String queueManagerName;
	private String queueName;
	private int[] values;
	private String startDate;
	private String startTime;
	private String endDate;
	private String endTime;
	
	public int getMonitoringType() {
		return monitoringType;
	}
	public void setMonitoringType(int type) {
		this.monitoringType = type;
	}		
	
	public int getType() {
		return pcfType;
	}
	public void setType(int type) {
		this.pcfType = type;
	}
	
	public String getQueueManagerName() {
		return queueManagerName;
	}
	public void setQueueManagerName(String qmgrName) {
		this.queueManagerName = qmgrName;
	}
	
	public String getQueueName() {
		return queueName;
	}
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	public synchronized int[] getValues() {
		return values;
	}
	public synchronized void setValues(int[] values) {
		this.values = values;
	}
	
	public void setStartDate(String v) {
		this.startDate = v;
	}
	public String getStartDate() {
		return this.startDate;
	}

	public void setStartTime(String v) {
		this.startTime = v;
	}
	public String getStartTime() {
		return this.startTime;
	}

	public void setEndDate(String v) {
		this.endDate = v;
	}
	public String getEndDate() {
		return this.endDate;
	}

	public void setEndTime(String v) {
		this.endTime = v;
	}
	public String getEndTime() {
		return this.endTime;
	}
}
