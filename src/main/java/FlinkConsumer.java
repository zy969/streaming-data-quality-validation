package consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;

public class FlinkConsumer {
    private static final Logger logger = LogManager.getLogger(FlinkConsumer.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");

        // Create Kafka consumer
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), props);

        // Consume data from Kafka topic
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);






        //简单写了个计算字符数，需要改为质量验证的逻辑






        // Use FlatMapFunction to count characters for each string
        DataStream<Tuple2<String, Integer>> characterCount = kafkaStream.flatMap(new CharacterCount());
        characterCount.print();

        try {
            env.execute("Flink Consumer");
        } catch (Exception e) {
            logger.error("An error occurred while executing Flink consumer.", e);
        }
    }

    public static class CharacterCount implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<>(value, value.length()));
            logger.info("String: " + value + ", Character Count: " + value.length());
        }
    }
}
