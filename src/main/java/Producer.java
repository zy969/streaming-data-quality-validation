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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;



public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);
    private static final String KAFKA_TOPIC = "topic";
    private static final String ZOOKEEPER_SERVER = "zookeeper:32181";
    private static final String KAFKA_SERVER = "kafka:9092";
    // Thread pool
    private static final ExecutorService executorService = Executors.newFixedThreadPool(10); 
    private static final ObjectMapper objectMapper = new ObjectMapper();

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

                        // Read and process each Parquet file
                        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
                            Group record;
                            while ((record = reader.read()) != null) {
                                // Send each record to Kafka
                                 String jsonRecord = convertGroupToJson(record);
                                sendRecordToKafka(producer, jsonRecord);
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

    // Check and create Kafka topic
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

    // Load producer properties
    private static Properties loadProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_SERVER);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return props;
    }

    // Send record to Kafka
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

    private static String convertGroupToJson(Group record) {
        ObjectNode jsonNode = objectMapper.createObjectNode();

        // Handle nullable string fields
        if (record.getBinary("dispatching_base_num", 0) != null) {
            jsonNode.put("dispatching_base_num", record.getString("dispatching_base_num", 0));
        } else {
            jsonNode.putNull("dispatching_base_num");
        }

        if (record.getBinary("Affiliated_base_number", 0) != null) {
            jsonNode.put("Affiliated_base_number", record.getString("Affiliated_base_number", 0));
        } else {
            jsonNode.putNull("Affiliated_base_number");
        }

        // Handle nullable long fields with logicalType timestamp-micros
        try {
            long pickupDatetime = record.getLong("pickup_datetime", 0);
            jsonNode.put("pickup_datetime", pickupDatetime);
        } catch (RuntimeException e) {
            jsonNode.putNull("pickup_datetime");
        }

        try {
            long dropOffDatetime = record.getLong("dropOff_datetime", 0);
            jsonNode.put("dropOff_datetime", dropOffDatetime);
        } catch (RuntimeException e) {
            jsonNode.putNull("dropOff_datetime");
        }

        // Handle nullable double fields
        try {
            Double PUlocationID = record.getDouble("PUlocationID", 0);
            if(PUlocationID != null) {
                jsonNode.put("PUlocationID", PUlocationID);
            } else {
                jsonNode.putNull("PUlocationID");
            }
        } catch (RuntimeException e) {
            jsonNode.putNull("PUlocationID");
        }

        try {
            Double DOlocationID = record.getDouble("DOlocationID", 0);
            if(DOlocationID != null) {
                jsonNode.put("DOlocationID", DOlocationID);
            } else {
                jsonNode.putNull("DOlocationID");
            }
        } catch (RuntimeException e) {
            jsonNode.putNull("DOlocationID");
        }

        // Handle nullable int fields
        try {
            Integer srFlag = record.getInteger("SR_Flag", 0);
            jsonNode.put("SR_Flag", srFlag);
        } catch (RuntimeException e) {
            jsonNode.putNull("SR_Flag");
        }

        // Convert the JSON object to a string
        try {
            return objectMapper.writeValueAsString(jsonNode);
        } catch (IOException e) {
            logger.error("Failed to convert record to JSON", e);
            return "{}"; 
        }
    }
}
