package consumer

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import java.util.Properties

class FlinkConsumer extends RichFlatMapFunction[String, Tuple2[String, Integer]] {
  private val logger = LogManager.getLogger(this.getClass)

  override def open(parameters: Configuration): Unit = {
    logger.info("FlinkConsumer started.")
  }

  override def flatMap(value: String, out: Collector[Tuple2[String, Integer]]): Unit = {
    logger.info(s"Received string: $value")
    // 添加数据质量验证的代码
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

    try {
      // Execute the Flink job
      env.execute("Flink Consumer")
      logger.info("Flink consumer execution started.")
    } catch {
      case e: Exception => logger.error("An error occurred while executing Flink consumer.", e)
    }
  }
}
