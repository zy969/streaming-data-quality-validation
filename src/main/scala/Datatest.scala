package consumer

import java.time.{ZoneId, ZonedDateTime}

class Datatype {
  val VendorID: Int = 0
  val lpep_pickup_datetime: ZonedDateTime = ZonedDateTime.now(ZoneId.of("America/New_York"))
  val lpep_dropoff_datetime: ZonedDateTime = ZonedDateTime.now(ZoneId.of("America/New_York"))
  val store_and_fwd_flag: String = ""
  val RatecodeID: Double = 0.0
  val PULocationID: Int = 0
  val DOLocationID: Int = 0
  val passenger_count: Double = 0.0
  val trip_distance: Double = 0.0
  val fare_amount: Double = 0.0
  val extra: Double = 0.0
  val mta_tax: Double = 0.0
  val tip_amount: Double = 0.0
  val tolls_amount: Double = 0.0
  val ehail_fee: Double = 0.0
  val improvement_surcharge: Double = 0.0
  val total_amount: Double = 0.0
  val payment_type: Double = 0.0
  val trip_type: Double = 0.0
  val congestion_surcharge: Double = 0.0
}

class Datatest(val value: String) {
  val types: Datatype = new Datatype

  def this(value: String, types: Datatype) {
    this(value)
    // Initialize types if necessary
  }
}