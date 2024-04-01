package consumer;

import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult;
import io.opencensus.trace.Link;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import com.stefan_grafberger.streamdq.VerificationSuite;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.flink.configuration.Configuration;

public class FlinkConsumer {
    private static final Logger logger = LogManager.getLogger(FlinkConsumer.class);
    public static List<List<RowLevelCheckResult<TaxiRideData>>> res = new LinkedList<>();
    public static void main(String[] args) {
        try {
            logger.info("Starting Flink Consumer application...");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            logger.info("Initialized StreamExecutionEnvironment.");

            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");
            logger.info("Kafka consumer properties set.");

            FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(),
                    props);

            logger.info("Created Kafka Consumer.");

            env.addSource(kafkaConsumer)
                    .process(new StreamDQValidator(res));
            logger.info("Added Kafka Consumer as source to the environment.");

            env.execute("Flink Consumer with StreamDQ Validation");
            logger.info("Flink Consumer application started.");

            logger.info("~~~~~~~~~~~~~~res~~~~~~~~~~~~~~~~~~~~~~~~~");
            logger.info(res.toString());
            logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        } catch (Exception e) {
            logger.error("An error occurred during the execution of the Flink Consumer application", e);
        }
    }

    public static class StreamDQValidator extends ProcessFunction<String, String> {
        private transient long lastTimeWindow;
        private transient int recordCount;
        private transient long totalLatency;
        private transient int latencyRecordCount;

        public List<List<RowLevelCheckResult<TaxiRideData>>> sub_res = null;
        StreamDQValidator(List<List<RowLevelCheckResult<TaxiRideData>>> res){
            this.sub_res = new LinkedList<>(res);
        }
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

                // 行级检查：确保乘客计数在0到5之间
                logger.debug("Initialized validation.");

                VerificationSuite verificationSuite = new VerificationSuite();
                this.sub_res.add(validation.getRes());


                long endProcessingTime = System.currentTimeMillis();
                long latency = endProcessingTime - startProcessingTime;
                totalLatency += latency;
                latencyRecordCount++;

                long now = System.currentTimeMillis();
                if (now - lastTimeWindow >= 1000) { // 每1000毫秒（1秒）计算一次吞吐量和平均延迟
                    double averageLatency = latencyRecordCount == 0 ? 0 : (double) totalLatency / latencyRecordCount;
                    logger.info("Throughput (records per second): {}, Average Latency: {} ms", recordCount,
                            averageLatency);

                    // 重置计数器、总延迟和时间窗口
                    recordCount = 0;
                    totalLatency = 0;
                    latencyRecordCount = 0;
                    lastTimeWindow = now;
                }
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
