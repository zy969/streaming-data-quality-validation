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

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck;
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy;
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck;
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck;
import com.stefan_grafberger.streamdq.VerificationSuite;

import java.math.BigDecimal;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

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
        @Override
        public void processElement(String jsonString, Context ctx, Collector<String> out) {
            logger.debug("Processing element: {}", jsonString);
            try {        
                Validation validation = new Validation(jsonString);

                // 行级检查：确保乘客计数在0到5之间
                logger.debug("Initialized validation.");

                VerificationSuite verificationSuite = new VerificationSuite();
                RowLevelCheck rowLevelCheck = new RowLevelCheck()
                .isInRange("passenger_count", BigDecimal.valueOf(0.0), BigDecimal.valueOf(5.0));

                // 将数据原样输出
                out.collect(jsonString);
                logger.debug("Element processed and collected: {}", jsonString);
            } catch (Exception e) {
                logger.error("Error processing element with StreamDQValidator", e);
                throw new RuntimeException("Error processing element with StreamDQValidator", e);
            }
        }
    }
}