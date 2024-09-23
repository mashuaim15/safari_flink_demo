package com.example.demo.config;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.annotation.*;

@Configuration
@ComponentScan(basePackages = "com.example.demo")
public class FlinkConfig {
    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.set(WebOptions.SUBMIT_ENABLE, true);

//        return StreamExecutionEnvironment.getExecutionEnvironment(configuration); // for external clusters

//        return StreamExecutionEnvironment.createRemoteEnvironment(
//                "flink-jobmanager-host",  // Replace with your Flink JobManager hostname or IP
//                8081,                     // JobManager port (default is 8081)
//                "path/to/your/job.jar"    // Path to your job JAR file
//        );
//        //for standalone testing

        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    }
}




