package com.example.demo.service;

import com.example.demo.config.FlinkConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.Objects;

@Component
public class StreamDataToMySqlJob implements CommandLineRunner, Serializable {
    private transient StreamExecutionEnvironment env;

    public StreamDataToMySqlJob() {
        FlinkConfig flinkConfig = new FlinkConfig();
        this.env = flinkConfig.streamExecutionEnvironment();
        ThreadLocal<Gson> gsonThreadLocal = ThreadLocal.withInitial(Gson::new);
    }

    @Override
    public void run(String... args) throws Exception {
        // Create Kafka source for control stream E
        KafkaSource<String> sourceE = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("manual_event_topic")
                .setGroupId("flink-processor-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> streamE = env.fromSource(sourceE, WatermarkStrategy.noWatermarks(), "Kafka Source E");

        // Process control stream E
        streamE.process(new ControlStreamProcessor(env)).print();

        env.execute("Stream Data to MySQL Job");
    }

    private static class ControlStreamProcessor extends ProcessFunction<String, String> {
        private transient ThreadLocal<Gson> gsonThreadLocal;
        private boolean isProcessing = false;
        private transient StreamExecutionEnvironment env;

        public ControlStreamProcessor(StreamExecutionEnvironment env) {
            this.env = env;
            this.gsonThreadLocal = ThreadLocal.withInitial(Gson::new);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            Gson gson = gsonThreadLocal.get();
            JsonObject jsonObject = gson.fromJson(value, JsonObject.class);
            String eventValue = jsonObject.get("value").getAsString();

            if ("start".equals(eventValue) && !isProcessing) {
                isProcessing = true;
                System.out.println("Started processing streams A, B, C, D");
                startProcessingStreams();
            } else if ("close".equals(eventValue) && isProcessing) {
                isProcessing = false;
                System.out.println("Stopped processing streams A, B, C, D");
                stopProcessingStreams();
            }

            out.collect(value);
        }

        private void startProcessingStreams() {
            // Create Kafka sources for streams A, B, C, D
            KafkaSource<String> sourceA = createKafkaSource("status_A_topic");
            KafkaSource<String> sourceB = createKafkaSource("status_B_topic");
            KafkaSource<String> sourceC = createKafkaSource("value_C_topic");
            KafkaSource<String> sourceD = createKafkaSource("value_D_topic");

            // Create data streams
            DataStream<MyRecord> streamA = env.fromSource(sourceA, WatermarkStrategy.noWatermarks(), "Kafka Source A")
                    .map(new JsonToRecordMapper());

            DataStream<MyRecord> streamB = env.fromSource(sourceB, WatermarkStrategy.noWatermarks(), "Kafka Source B")
                    .map(new JsonToRecordMapper())
                    .map(new TimestampAdjuster(-100)); // Adjust for 100ms delay

            DataStream<MyRecord> streamC = env.fromSource(sourceC, WatermarkStrategy.noWatermarks(), "Kafka Source C")
                    .map(new JsonToRecordMapper())
                    .map(new TimestampAdjuster(-150)); // Adjust for 150ms delay

            DataStream<MyRecord> streamD = env.fromSource(sourceD, WatermarkStrategy.noWatermarks(), "Kafka Source D")
                    .map(new JsonToRecordMapper())
                    .map(new TimestampAdjuster(-1000)); // Adjust for 1s delay

            // Union and synchronize streams C and D
            DataStream<MyRecord> streamCD = streamC
                    .union(streamD)
                    .keyBy(record -> record.getName())
                    .process(new SynchronizationFunction());

            // Process streams A and B to get status
            DataStream<Boolean> statusStream = streamA
                    .connect(streamB)
                    .flatMap(new StatusFunction());

            // Apply conditional processing
            DataStream<MyRecord> streamE = statusStream
                    .connect(streamCD)
                    .process(new ConditionalProcessFunction());

            // Write the results to MySQL
            streamA.addSink(createMySqlSink("stream_data_a"));
            streamB.addSink(createMySqlSink("stream_data_b"));
            streamC.addSink(createMySqlSink("stream_data_c"));
            streamD.addSink(createMySqlSink("stream_data_d"));
            streamE.addSink(createMySqlSink("stream_data_e"));
        }

        private void stopProcessingStreams() {
            // Implement logic to stop processing streams
            // This might involve cancelling jobs or clearing state
        }
    }

    private static KafkaSource<String> createKafkaSource(String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-processor-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static SinkFunction<MyRecord> createMySqlSink(String tableName) {
        return JdbcSink.sink(
                "INSERT INTO " + tableName + " (name, value, timestamp) VALUES (?, ?, ?)",
                (statement, record) -> {
                    statement.setString(1, record.getName());
                    statement.setDouble(2, record.getValue());
                    statement.setLong(3, record.getTimestamp());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/your_database_name")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("your_username")
                        .withPassword("your_password")
                        .build()
        );
    }

    private static class JsonToRecordMapper implements MapFunction<String, MyRecord>, Serializable {
        private transient ThreadLocal<Gson> gsonThreadLocal;

        public JsonToRecordMapper() {
            this.gsonThreadLocal = ThreadLocal.withInitial(Gson::new);
        }

        @Override
        public MyRecord map(String value) throws Exception {
            Gson gson = gsonThreadLocal.get();
            JsonObject jsonObject = gson.fromJson(value, JsonObject.class);
            String name = jsonObject.get("name").getAsString();
            double doubleValue = jsonObject.get("value").getAsDouble();
            long timestamp = jsonObject.get("timestamp").getAsLong();
            return new MyRecord(name, doubleValue, timestamp);
        }
    }

    private static class TimestampAdjuster implements MapFunction<MyRecord, MyRecord>, Serializable {
        private final long adjustment;

        public TimestampAdjuster(long adjustment) {
            this.adjustment = adjustment;
        }

        @Override
        public MyRecord map(MyRecord value) {
            return new MyRecord(value.getName(), value.getValue(), value.getTimestamp() + adjustment);
        }
    }
    // sync by timestamp
    private static class SynchronizationFunction extends ProcessFunction<MyRecord, MyRecord> {
        private MapState<String, MyRecord> buffer;
        private final long maxOutOfOrderness = 3000; // 3 seconds

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, MyRecord> descriptor =
                    new MapStateDescriptor<>("buffer", Types.STRING, Types.POJO(MyRecord.class));
            buffer = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(MyRecord value, Context ctx, Collector<MyRecord> out) throws Exception {
            String streamName = value.getName();
            long currentWatermark = ctx.timerService().currentWatermark();

            // Store the current record
            buffer.put(streamName, value);

            // Check if we have records from both streams
            if (buffer.contains("C") && buffer.contains("D")) {
                MyRecord recordC = buffer.get("C");
                MyRecord recordD = buffer.get("D");

                // Align timestamps
                long alignedTimestamp = Math.max(recordC.getTimestamp(), recordD.getTimestamp());

                // Emit aligned records if they are within the allowed lateness
                if (alignedTimestamp <= currentWatermark + maxOutOfOrderness) {
                    out.collect(new MyRecord("C", recordC.getValue(), alignedTimestamp));
                    out.collect(new MyRecord("D", recordD.getValue(), alignedTimestamp));
                    // Clear the buffer
                    buffer.clear();
                }
            }
        }
    }

    private static class StatusFunction implements CoFlatMapFunction<MyRecord, MyRecord, Boolean> {
        private boolean statusA = false;
        private boolean statusB = false;

        @Override
        public void flatMap1(MyRecord value, Collector<Boolean> out) throws Exception {
            statusA = value.getValue() == 1.0;
            out.collect(statusA && statusB);
        }

        @Override
        public void flatMap2(MyRecord value, Collector<Boolean> out) throws Exception {
            statusB = value.getValue() == 1.0;
            out.collect(statusA && statusB);
        }
    }

    private static class ConditionalProcessFunction extends CoProcessFunction<Boolean, MyRecord, MyRecord> {
        private Boolean lastStatus = null;
        private MyRecord lastC = null;
        private MyRecord lastD = null;

        @Override
        public void processElement1(Boolean status, Context ctx, Collector<MyRecord> out) throws Exception {
            lastStatus = status;
            if (lastC != null && lastD != null) {
                processAndEmit(out);
            }
        }

        @Override
        public void processElement2(MyRecord value, Context ctx, Collector<MyRecord> out) throws Exception {
            if ("C".equals(value.getName())) {
                lastC = value;
            } else if ("D".equals(value.getName())) {
                lastD = value;
            }

            if (lastStatus != null && lastC != null && lastD != null) {
                processAndEmit(out);
            }
        }

        private void processAndEmit(Collector<MyRecord> out) {
            if (lastStatus) {
                // When both status A and B are 1, add C and D
                double result = lastC.getValue() + lastD.getValue();
                out.collect(new MyRecord("E", result, Math.max(lastC.getTimestamp(), lastD.getTimestamp())));
            } else {
                // When status A or B are 0, divide C by D
                double result = lastC.getValue() / lastD.getValue();
                out.collect(new MyRecord("E", result, Math.max(lastC.getTimestamp(), lastD.getTimestamp())));
            }
            lastC = null;
            lastD = null;
        }
    }

    public static class MyRecord implements Serializable {
        private final String name;
        private final Double value;
        private final Long timestamp;

        public MyRecord(String name, Double value, Long timestamp) {
            this.name = name;
            this.value = value;
            this.timestamp = timestamp;
        }

        public String getName() { return name; }
        public Double getValue() { return value; }
        public Long getTimestamp() { return timestamp; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyRecord myRecord = (MyRecord) o;
            return Objects.equals(name, myRecord.name) &&
                    Objects.equals(value, myRecord.value) &&
                    Objects.equals(timestamp, myRecord.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value, timestamp);
        }

        @Override
        public String toString() {
            return "MyRecord{" +
                    "name='" + name + '\'' +
                    ", value=" + value +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}