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
        ExecutorService executor = Executors.newFixedThreadPool(2); // 创建一个包含两个线程的线程池

        try {
            // 异步运行Kafka生产者
            executor.submit(() -> {
                logger.info("Starting Kafka producer...");
                Producer.main(new String[]{});
                logger.debug("Kafka producer started successfully.");
            });

            // 异步运行Flink消费者
            executor.submit(() -> {
                logger.info("Starting Flink consumer...");
                FlinkConsumer.main(new String[]{});
                logger.debug("Flink consumer started successfully.");
            });

            executor.shutdown(); // 关闭ExecutorService，不再接收新的任务
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // 等待所有任务完成
        } catch (Exception e) {
            logger.error("An error occurred during the execution: ", e);
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow(); // 尝试立即关闭所有正在执行的任务
            }
            logger.info("Both Kafka producer and Flink consumer tasks submitted.");
        }
    }
}
