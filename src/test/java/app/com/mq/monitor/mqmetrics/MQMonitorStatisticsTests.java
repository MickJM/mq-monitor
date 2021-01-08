package app.com.mq.monitor.mqmetrics;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import com.ibm.mq.MQException;
import com.ibm.mq.headers.MQDataException;

import app.com.mq.json.entities.Metric;
import app.com.mq.monitor.mqmetrics.MQConnection;
import app.com.mq.monitor.mqmetrics.MQMetricsApplication;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Meter.Id;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { MQMetricsApplication.class }, 
	properties = { "ibm.mq.queueName=SYSTEM.ADMIN.STATISTICS.QUEUE", "ibm.mq.pcf.accountingType=MQCFT_STATISTICS" } )
@Component
@ActiveProfiles("test")
public class MQMonitorStatisticsTests {

	static Logger log = LoggerFactory.getLogger(MQMonitorStatisticsTests.class);
	
	@Autowired
	private MQConnection conn;
	
	@Autowired
	private MeterRegistry meterRegistry;

	@Value("${ibm.mq.queueManager}")
	private String queueManager;
	public void setQueueManager(String v) {
		this.queueManager = v;
	}
	public String getQueueManagerName() { return this.queueManager; }

	
	@Test
	@Order(3)
	public void StatisticsTests() throws MQDataException, ParseException, 
			MQException, IOException, InterruptedException {

		log.info("Attempting to connect to {}", getQueueManagerName());		
		Thread.sleep(2000);

		this.conn.GetMetrics();
		
		List<Meter.Id> filter = this.meterRegistry.getMeters().stream()
		        .map(Meter::getId)
		        .collect(Collectors.toList());

		Comparator<Meter.Id> byType = (Id a, Id b) -> (a.getName().compareTo(b.getName()));
		Collections.sort(filter, byType);
		
		Iterator<Id> list = filter.iterator();
		assert(list) != null : "no metrics were returned";
		
		int mqMetrics = 0;
		while (list.hasNext()) {
			Meter.Id id = list.next();
			if (id.getName().startsWith("mq:")) {
				mqMetrics++;
			}

			Metric m = new Metric();
			m.setName(id.getName());

			List<Tag> tags = id.getTags();
			
			if (id.getType() == Meter.Type.GAUGE) {
				Gauge g = this.meterRegistry.find(id.getName()).tags(tags).gauge();
				assert(g) != null;
			}
			
		}
		log.info("mq metrics count: {}", mqMetrics);
		assert (mqMetrics > 0) : "No mq: metrics generated - connection seems fine, may be STATISTICS accounting is switch off";
			
	}
	
	
}
