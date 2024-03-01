package consumer;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class FlinkConsumer {
    private static final Logger logger = LogManager.getLogger(FlinkConsumer.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");
        logger.info("Connecting to Kafka...");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), props);
        logger.info("Kafka consumer created for topic 'topic'.");

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
        logger.info("Started consuming data from Kafka.");






         //简单写了个计算字符数，需要改为质量验证的逻辑







        DataStream<Tuple2<String, Integer>> characterCount = kafkaStream.flatMap(new RichCharacterCount());
        characterCount.print();

        try {
            env.execute("Flink Consumer");
            logger.info("Flink consumer execution started.");
        } catch (Exception e) {
            logger.error("An error occurred while executing Flink consumer.", e);
        }
    }

    public static class RichCharacterCount extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void open(Configuration parameters) {
            logger.info("FlatMap function started.");
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<>(value, value.length()));
            logger.info("Processed string: " + value + ", Length: " + value.length());
        }

        @Override
        public void close() {
            logger.info("FlatMap function closed.");
        }
    }
}







