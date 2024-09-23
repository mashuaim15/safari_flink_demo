//package com.example.demo;
//
//import com.example.demo.service.StreamDataToMySqlJob;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.context.ConfigurableApplicationContext;
//
//@SpringBootApplication
//public class KafkaFlinkDemoApplication {
//
//	public static void main(String[] args) throws Exception {
//		ConfigurableApplicationContext context = SpringApplication.run(KafkaFlinkDemoApplication.class, args);
//
//		// Get the bean and run it
//		StreamDataToMySqlJob streamDataToMySqlJob = context.getBean(StreamDataToMySqlJob.class);
//		try {
//			streamDataToMySqlJob.run(args);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//}

package com.example.demo;

import com.example.demo.service.StreamDataToMySqlJob;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class KafkaFlinkDemoApplication {
	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaFlinkDemoApplication.class, args);
		StreamDataToMySqlJob job = context.getBean(StreamDataToMySqlJob.class);
		try {
			job.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
