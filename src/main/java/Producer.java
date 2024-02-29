package producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);
    private static final String KAFKA_TOPIC = "topic";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final ExecutorService executorService = Executors.newFixedThreadPool(10); 

    public static void main(String[] args) {
        createKafkaTopic();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(loadProducerProperties())) {
            File datasetsDirectory = new File("dataset");
            logger.debug("Looking for files in directory: " + datasetsDirectory.getAbsolutePath());

            File[] files = datasetsDirectory.listFiles();
            if (files != null && files.length > 0) {
                logger.debug("Found " + files.length + " files in dataset directory.");

                for (File file : files) {
                    logger.debug("Processing file: " + file.getName());

                    if (file.getName().endsWith(".parquet")) {
                        Path path = new Path(file.getAbsolutePath());

                        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
                            Group record;
                            while ((record = reader.read()) != null) {
                                sendRecordToKafka(producer, record.toString());
                            }
                        }
                    } else {
                        logger.warn("Skipping non-parquet file: " + file.getName());
                    }
                }
            } else {
                logger.error("No files found in dataset directory.");
            }
        } catch (IOException e) {
            logger.error("Error reading Parquet file: ", e);
        }
        executorService.shutdown();
    }

    private static void createKafkaTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        try (AdminClient adminClient = AdminClient.create(props)) {
            logger.debug("Creating NewTopic object...");
            NewTopic newTopic = new NewTopic(KAFKA_TOPIC, 1, (short) 1);
            Future<Void> future = adminClient.createTopics(Collections.singletonList(newTopic)).all();
            logger.debug("Waiting for topic creation to complete...");
            future.get();
            logger.info("Kafka topic " + KAFKA_TOPIC + " created.");
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                logger.info("Kafka topic " + KAFKA_TOPIC + " already exists.");
            } else {
                logger.error("Failed to create Kafka topic: " + e.getMessage(), e);
            }
        }
    }

    private static Properties loadProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_SERVER);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return props;
    }

    private static void sendRecordToKafka(KafkaProducer<String, String> producer, String record) {
        executorService.submit(() -> producer.send(new ProducerRecord<>(KAFKA_TOPIC, record), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    logger.error("Failed to send record to Kafka", e);
                } else {
                    logger.debug("Record sent to Kafka topic: " + metadata.topic() + " with offset: " + metadata.offset());
                }
            }
        }));
    }
}
