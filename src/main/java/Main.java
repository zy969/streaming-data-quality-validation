package main;

import consumer.FlinkConsumer;
import producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(final String... args) {
        try {
            logger.info("Starting Kafka producer...");
            Producer.main(new String[]{});

            logger.info("Starting Flink consumer...");
            FlinkConsumer.main(new String[]{});

            logger.debug("Both Kafka producer and Flink consumer started successfully.");
        } catch (Exception e) {
            logger.error("An error occurred during the execution: ", e);
        }
    }
}
