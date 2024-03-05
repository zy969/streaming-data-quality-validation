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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import com.google.api.gax.paging.Page;




public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);
    private static final String KAFKA_TOPIC = "topic";
    private static final String ZOOKEEPER_SERVER = "zookeeper:32181";
    private static final String KAFKA_SERVER = "kafka:9092";
    private static final String JSON_KEY_PATH = "thermal-formula-416221-d4e3524907bf.json";

    // Thread pool
    private static final ExecutorService executorService = Executors.newFixedThreadPool(10); 
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        try {
            createKafkaTopic();
    
            GoogleCredentials credentials = null;
            try (FileInputStream serviceAccountStream = new FileInputStream(JSON_KEY_PATH)) {
                credentials = GoogleCredentials.fromStream(serviceAccountStream);
            } catch (IOException e) {
                logger.error("Failed to load Google credentials: ", e);
                return;
            }
    
            Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            String bucketName = "streaming-data-quality-validation";
    
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(loadProducerProperties())) {
                Page<Blob> blobs = storage.list(bucketName);
                for (Blob blob : blobs.iterateAll()) {
                    String fileName = blob.getName();
                    if (fileName.endsWith(".parquet")) {
                        logger.debug("Processing file: " + fileName);
    
                        Path path = new Path("gs://" + bucketName + "/" + fileName);
                   
                        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
                            Group record;

                            while ((record = reader.read()) != null) {
                                //Convert the record to JSON string
                                String json = convertGroupToJson(record);
                                // Send the JSON string to Kafka
                                sendRecordToKafka(producer, json);                
                            }
                            logger.debug("Finished processing Parquet file: " + fileName);
                        } catch (IOException e) {
                            logger.error("Error reading Parquet file from GCS: ", e);
                        }
                    }
                }
            }  finally {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executorService.shutdownNow();
                }
            }
        } catch (Exception e) {
            logger.error("An unexpected error occurred: ", e);
        } catch (Throwable t) {
            logger.error("A severe error occurred: ", t);
        }
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
