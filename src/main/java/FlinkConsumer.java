package consumer;

import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult;
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Properties;

public class FlinkConsumer {
    private static final Logger logger = LogManager.getLogger(FlinkConsumer.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");
        logger.info("Connecting to Kafka...");

        // Create a consumer
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), props);
        logger.info("Kafka consumer created for topic 'topic'.");

        // Add Kafka consumer as a source
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
        logger.info("Started consuming data from Kafka.");

        // Apply a flatMap transformation
        DataStream<Tuple2<String, Integer>> qualityCheck = kafkaStream.flatMap(new DataQualityValidator());
        qualityCheck.print();
        try {
        //use checker
        logger.info("```````````````````hello1``````````````````````");
        Validation valid = new Validation(env, kafkaStream);
        logger.info("```````````````````hello2``````````````````````");
        List<RowLevelCheckResult<String>> res =valid.getRes();
        logger.info("```````````````````hello3``````````````````````");
        logger.info(res.toString());
        logger.info("```````````````````hello4``````````````````````");

            // Execute the Flink job
            env.execute("Flink Consumer");
            logger.info("Flink consumer execution started.");
        } catch (Exception e) {
            logger.error("An error occurred while executing Flink consumer.", e);
        }
    }

    // Custom FlatMapFunction for data quality validation
    public static class DataQualityValidator extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void open(Configuration parameters) {
            logger.info("DataQualityValidator started.");
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 实现数据质量验证逻辑

            logger.info("Received string: " + value);
        }

        @Override
        public void close() {
            logger.info("DataQualityValidator function closed.");
        }
    }
}