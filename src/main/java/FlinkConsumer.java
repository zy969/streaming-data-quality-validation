package consumer;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck;
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy;
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck;
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck;
import com.stefan_grafberger.streamdq.VerificationSuite;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class FlinkConsumer {
    private static final Logger logger = LogManager.getLogger(FlinkConsumer.class);

    public static void main(String[] args) {
        try {
            logger.info("Starting Flink Consumer application...");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            logger.info("Initialized StreamExecutionEnvironment.");

            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");
            logger.info("Kafka consumer properties set.");

            FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), props);

            logger.info("Created Kafka Consumer.");

            env.addSource(kafkaConsumer)
                    .process(new StreamDQValidator());
            logger.info("Added Kafka Consumer as source to the environment.");

            env.execute("Flink Consumer with StreamDQ Validation");
            logger.info("Flink Consumer application started.");
        } catch (Exception e) {
            logger.error("An error occurred during the execution of the Flink Consumer application", e);
        }
    }

    public static class StreamDQValidator extends ProcessFunction<String, String> {
        private transient long lastTimeWindow;
        private transient int recordCount;
        private transient long totalLatency;
        private transient int latencyRecordCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTimeWindow = System.currentTimeMillis();
            recordCount = 0;
            totalLatency = 0;
            latencyRecordCount = 0;
        }

        @Override
        public void processElement(String jsonString, Context ctx, Collector<String> out) {
            logger.debug("Processing element: {}", jsonString);
            long startProcessingTime = System.currentTimeMillis();
            try {
                recordCount++;
                Validation validation = new Validation(jsonString);
                logger.debug("Initialized validation.");

                VerificationSuite verificationSuite = new VerificationSuite();

                long endProcessingTime = System.currentTimeMillis();
                long latency = endProcessingTime - startProcessingTime;
                totalLatency += latency;
                latencyRecordCount++;

                long now = System.currentTimeMillis();
                if (now - lastTimeWindow >= 1000) { // Calculate throughput and average latency every 1000 milliseconds (1 second)
                    double averageLatency = latencyRecordCount == 0 ? 0 : (double) totalLatency / latencyRecordCount;
                    logger.info("Throughput (records per second): {}, Average Latency: {} ms", recordCount,
                            averageLatency);

                    // Reset counters, total latency, and time window
                    recordCount = 0;
                    totalLatency = 0;
                    latencyRecordCount = 0;
                    lastTimeWindow = now;
                }
                // Output data (Debug):
                //out.collect(jsonString);
                //logger.debug("Element processed and collected: {}", jsonString);
            } catch (Exception e) {
                logger.error("Error processing element with StreamDQValidator", e);
                throw new RuntimeException("Error processing element with StreamDQValidator", e);
            }
        }
    }
}
