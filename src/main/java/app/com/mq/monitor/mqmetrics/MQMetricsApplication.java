package app.com.mq.monitor.mqmetrics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@ComponentScan(basePackages = { "maersk.com.mq.monitor.mqmetrics"} )
@ComponentScan("maersk.com.mq.monitor.stats")
@SpringBootApplication
@EnableScheduling
public class MQMetricsApplication {

	public static void main(String[] args) {
		SpringApplication sa = new SpringApplication(MQMetricsApplication.class);
		sa.run(args);
		
	}
}
