package consumer

import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.logging.log4j.LogManager

import java.util.Properties

/*class FlinkConsumer extends RichFlatMapFunction[String, Tuple2[String, Integer]] {
  private val logger = LogManager.getLogger(this.getClass)
  private var inputDataStream: DataStream[String] = _
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  override def open(parameters: Configuration): Unit = {
    logger.info("FlinkConsumer started.")

  }

  override def flatMap(value: String, out: Collector[Tuple2[String, Integer]]): Unit = {
    logger.info(s"Received string: $value")
    //change struct
    val cleaned_data = value.replaceAll("[{}\"]", "")
    val parts = cleaned_data.split(",")
    val eData = parts.map { part =>
      val keyValue = part.split(":")
      if (keyValue.length == 2) Some(keyValue(1)) else None
    }.flatten
    //save as taxiridedata
    val taxi_data_stream = env.fromElements(
      TaxiRideData(
        eData(0),eData(1),eData(2),eData(3),eData(4),eData(5),
        eData(6),eData(7),eData(8),eData(9),eData(10),eData(11),
        eData(12),eData(13),eData(14),eData(15),eData(16),eData(17),
        eData(18),eData(19)
      )
    )
    val valid = new Validation(env, taxi_data_stream)


    out.collect(new Tuple2(value, 1))
  }

  override def close(): Unit = {
    logger.info("FlinkConsumer closed.")
  }
}

object FlinkConsumer {
  private val logger = LogManager.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group")
    logger.info("Connecting to Kafka...")

    // Create a consumer
    val kafkaConsumer = new FlinkKafkaConsumer010[String]("topic", new SimpleStringSchema(), props)
    logger.info("Kafka consumer created for topic 'topic'.")

    // Add Kafka consumer as a source 
    val kafkaStream = env.addSource(kafkaConsumer)

    logger.info("Started consuming data from Kafka.")

    // Apply a flatMap transformation
    val qualityCheck = kafkaStream.flatMap(new FlinkConsumer)
    qualityCheck.print()
    //no key?
    //val keyedStream = kafkaStream.keyBy { ->}


    try {
      // Execute the Flink job
      env.execute("Flink Consumer")
      logger.info("Flink consumer execution started.")
    } catch {
      case e: Exception => logger.error("An error occurred while executing Flink consumer.", e)
    }
  }
}*/
