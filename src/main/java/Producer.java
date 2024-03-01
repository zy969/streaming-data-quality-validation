package producer;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.hadoop.fs.Path; 
import org.apache.parquet.hadoop.ParquetReader; 
import org.apache.parquet.example.data.Group; 
import org.apache.parquet.hadoop.example.GroupReadSupport; 
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.io.IOException; 
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);
    private static final String KAFKA_TOPIC = "topic";
    private static final String ZOOKEEPER_SERVER = "zookeeper:32181";
    private static final String KAFKA_SERVER = "kafka:9092";
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
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(ZOOKEEPER_SERVER, 20000, 20000, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(ZOOKEEPER_SERVER), false);
            if (!AdminUtils.topicExists(zkUtils, KAFKA_TOPIC)) {
                AdminUtils.createTopic(zkUtils, KAFKA_TOPIC, 1, 1, new Properties(), RackAwareMode.Safe$.MODULE$);
                logger.info("Kafka topic " + KAFKA_TOPIC + " created.");
            } else {
                logger.info("Kafka topic " + KAFKA_TOPIC + " already exists.");
            }
        } catch (Exception e) {
            logger.error("Exception occurred while creating Kafka topic: ", e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
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
                    //logger.debug("Record sent to Kafka topic: " + metadata.topic() + " with offset: " + metadata.offset());
                }
            }
        }));
    }
}
