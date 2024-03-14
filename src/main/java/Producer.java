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
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    logger.error("Interrupted while sleeping between Kafka messages", e);
                                    // Optional: Decide how to handle interruption (e.g., break out of the loop)
                                    break;
                                }
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
    // Send record to Kafka with a 1-second delay after each send
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


    private static String convertGroupToJson(Group record) {
//        green_tripdata.parquet
//        VendorID int32
//        lpep_pickup_datetime datetime64[us]
//        lpep_dropoff_datetime datetime64[us]
//        store_and_fwd_flag object
//        RatecodeID float64
//        PULocationID int32
//        DOLocationID int32
//        passenger_count float64
//        trip_distance float64
//        fare_amount float64
//        extra float64
//        mta_tax float64
//        tip_amount float64
//        tolls_amount float64
//        ehail_fee float64
//        improvement_surcharge float64
//        total_amount float64
//        payment_type float64
//        trip_type float64
//        congestion_surcharge float64
        ObjectNode jsonNode = objectMapper.createObjectNode();

        // Handle nullable string fields
        if (record.getBinary("store_and_fwd_flag", 0) != null) {
            jsonNode.put("store_and_fwd_flag", record.getString("store_and_fwd_flag", 0));
        } else {
            jsonNode.putNull("store_and_fwd_flag");
        }

        // Handle nullable long fields with logicalType timestamp-micros
        try {
            Long lpep_pickup_datetime = record.getLong("lpep_pickup_datetime", 0);
            jsonNode.put("lpep_pickup_datetime", lpep_pickup_datetime);
        } catch (RuntimeException e) {
            jsonNode.putNull("lpep_pickup_datetime");
        }

        try {
            Long lpep_dropoff_datetime = record.getLong("lpep_dropoff_datetime", 0);
            jsonNode.put("lpep_dropoff_datetime", lpep_dropoff_datetime);
        } catch (RuntimeException e) {
            jsonNode.putNull("lpep_dropoff_datetime");
        }

        // Handle nullable double fields
        try {
            Double RateCodeID = record.getDouble("RateCodeID", 0);
            jsonNode.put("RateCodeID", RateCodeID);
        } catch (RuntimeException e) {
            jsonNode.putNull("RateCodeID");
        }

        try {
            Double passenger_count = record.getDouble("passenger_count", 0);
            jsonNode.put("passenger_count", passenger_count);
        } catch (RuntimeException e) {
            jsonNode.putNull("passenger_count");
        }

        try {
            Double trip_distance = record.getDouble("trip_distance", 0);
            jsonNode.put("trip_distance", trip_distance);
        } catch (RuntimeException e) {
            jsonNode.putNull("trip_distance");
        }

        try {
            Double fare_amount = record.getDouble("fare_amount", 0);
            jsonNode.put("fare_amount", fare_amount);
        } catch (RuntimeException e) {
            jsonNode.putNull("fare_amount");
        }

        try {
            Double extra = record.getDouble("extra", 0);
            jsonNode.put("extra", extra);
        } catch (RuntimeException e) {
            jsonNode.putNull("extra");
        }

        try {
            Double mta_tax = record.getDouble("mta_tax", 0);
            jsonNode.put("mta_tax", mta_tax);
        } catch (RuntimeException e) {
            jsonNode.putNull("mta_tax");
        }

        try {
            Double tip_amount = record.getDouble("tip_amount", 0);
            jsonNode.put("tip_amount", tip_amount);
        } catch (RuntimeException e) {
            jsonNode.putNull("tip_amount");
        }

        try {
            Double tolls_amount = record.getDouble("tolls_amount", 0);
            jsonNode.put("tolls_amount", tolls_amount);
        } catch (RuntimeException e) {
            jsonNode.putNull("tolls_amount");
        }

        try {
            Double ehail_fee = record.getDouble("ehail_fee", 0);
            jsonNode.put("ehail_fee", ehail_fee);
        } catch (RuntimeException e) {
            jsonNode.putNull("ehail_fee");
        }

        try {
            Double improvement_surcharge = record.getDouble("improvement_surcharge", 0);
            jsonNode.put("improvement_surcharge", improvement_surcharge);
        } catch (RuntimeException e) {
            jsonNode.putNull("improvement_surcharge");
        }

        try {
            Double total_amount = record.getDouble("total_amount", 0);
            jsonNode.put("total_amount", total_amount);
        } catch (RuntimeException e) {
            jsonNode.putNull("total_amount");
        }

        try {
            Double payment_type = record.getDouble("payment_type", 0);
            jsonNode.put("payment_type", payment_type);
        } catch (RuntimeException e) {
            jsonNode.putNull("payment_type");
        }

        try {
            Double trip_type = record.getDouble("trip_type", 0);
            jsonNode.put("trip_type", trip_type);
        } catch (RuntimeException e) {
            jsonNode.putNull("trip_type");
        }

        try {
            Double congestion_surcharge = record.getDouble("congestion_surcharge", 0);
            jsonNode.put("congestion_surcharge", congestion_surcharge);
        } catch (RuntimeException e) {
            jsonNode.putNull("congestion_surcharge");
        }


        // Handle nullable int fields
        try {
            Integer VendorID = record.getInteger("VendorID", 0);
            jsonNode.put("VendorID", VendorID);
        } catch (RuntimeException e) {
            jsonNode.putNull("VendorID");
        }

        try {
            Integer PULocationID = record.getInteger("PULocationID", 0);
            jsonNode.put("PULocationID", PULocationID);
        } catch (RuntimeException e) {
            jsonNode.putNull("PULocationID");
        }

        try {
            Integer DOLocationID = record.getInteger("DOLocationID", 0);
            jsonNode.put("DOLocationID", DOLocationID);
        } catch (RuntimeException e) {
            jsonNode.putNull("DOLocationID");
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
