package main;

import consumer.FlinkConsumer;
import producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(final String... args) {
        ExecutorService executor = Executors.newFixedThreadPool(2); // Thread pool

        try {
            // Asynchronously run Kafka producer
            executor.submit(() -> {
                logger.info("Starting Kafka producer...");
                Producer.main(new String[]{});
                logger.debug("Kafka producer started successfully.");
            });

            // Asynchronously run Flink consumer
            executor.submit(() -> {
                logger.info("Starting Flink consumer...");
                try {
                    FlinkConsumer.main(new String[]{});
                    logger.debug("Flink consumer started successfully.");
                } catch (Exception e) {
                    logger.error("An error occurred during Flink consumer execution: ", e);
                }
            });

            executor.shutdown(); 
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); 
        } catch (Exception e) {
            logger.error("An error occurred during the execution: ", e);
        } finally {
            // Force shutdown if not all tasks finished
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
            logger.info("Both Kafka producer and Flink consumer tasks submitted.");
        }
    }
}
