//package com.example.demo.service;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//@Service
//public class FlinkJobService {
//    private final StreamExecutionEnvironment env;
//
//    @Autowired
//    public FlinkJobService(StreamExecutionEnvironment env) {
//        this.env = env;
//    }
//
//    public void runFlinkJob() throws Exception {
//        DataStream<String> dataStream = env.fromElements("Hello", "Flink", "in", "Spring", "Boot");
//        dataStream.print();
//        env.execute("Flink job in Spring Boot");
//    }
//}
