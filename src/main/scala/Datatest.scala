package consumer

import java.time.{ZoneId, ZonedDateTime}
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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import com.stefan_grafberger.streamdq.VerificationSuite

class Datatype {
  var VendorID: Int = 0
  var lpep_pickup_datetime: Long = 0L
  var lpep_dropoff_datetime: Long = 0L
  var store_and_fwd_flag: String = ""
  var RatecodeID: Double = 0.0
  var PULocationID: Int = 0
  var DOLocationID: Int = 0
  var passenger_count: Double = 0.0
  var trip_distance: Double = 0.0
  var fare_amount: Double = 0.0
  var extra: Double = 0.0
  var mta_tax: Double = 0.0
  var tip_amount: Double = 0.0
  var tolls_amount: Double = 0.0
  var ehail_fee: Double = 0.0
  var improvement_surcharge: Double = 0.0
  var total_amount: Double = 0.0
  var payment_type: Double = 0.0
  var trip_type: Double = 0.0
  var congestion_surcharge: Double = 0.0
}

class Datatest(val value: String, val types: Datatype) {
  // Initialization logic goes here, if necessary
  val data1 = new Datatype()
  val cleaned_data = value.replaceAll("[{}\"]", "")
  val parts = cleaned_data.split(",")
  val extractedData = parts.map { part =>
    val keyValue = part.split(":")
    if (keyValue.length == 2) Some(keyValue(1)) else None
  }.flatten
  //data validate
  data1.store_and_fwd_flag = extractedData(0)
  data1.lpep_pickup_datetime = extractedData(1).toLong
  data1.lpep_dropoff_datetime = extractedData(2).toLong
  data1.RatecodeID = extractedData(3).toDouble // potential null
  data1.passenger_count = extractedData(4).toDouble // must be integer?
  data1.trip_distance = extractedData(5).toDouble
  data1.fare_amount = extractedData(6).toDouble
  data1.extra = extractedData(7).toDouble
  data1.mta_tax = extractedData(8).toDouble
  data1.tip_amount = extractedData(9).toDouble
  data1.tolls_amount = extractedData(10).toDouble
  data1.ehail_fee = extractedData(11).toDouble
  data1.improvement_surcharge = extractedData(12).toDouble
  data1.total_amount = extractedData(13).toDouble
  data1.payment_type = extractedData(14).toDouble
  data1.trip_type = extractedData(15).toDouble
  data1.congestion_surcharge = extractedData(16).toDouble
  data1.VendorID = extractedData(17).toInt
  data1.PULocationID = extractedData(18).toInt
  data1.DOLocationID = extractedData(19).toInt
}