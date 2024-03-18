package consumer
import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.types.Row
import java.math.BigDecimal

data class TaxiRideData @JvmOverloads constructor(
    var store_and_fwd_flag: String = "",
    var lpep_pickup_datetime:Long = 0L,
    var lpep_dropoff_datetime:Long = 0L,
    var RateCodeID: Double = 0.0,
    var passenger_count: Double = 0.0,
    var trip_distance: Double = 0.0,
    var fare_amount: Double = 0.0,
    var extra: Double = 0.0,
    var mta_tax: Double = 0.0,
    var tip_amount: Double = 0.0,
    var tolls_amount: Double = 0.0,
    var ehail_fee: Double = 0.0,
    var improvement_surcharge: Double = 0.0,
    var total_amount: Double = 0.0,
    var payment_type: Double = 0.0,
    var trip_type: Double = 0.0,
    var congestion_surcharge: Double = 0.0,
    var VendorID: Int = 0,
    var PULocationID:Int = 0,
    var DOLocationID:Int = 0
)

class Validation(var env: StreamExecutionEnvironment, var stream: DataStream<String>){
        var data_out_of_range: String = "Data_out_of_range"
        var data_invalid_value: String = "Data_invalid_value"


        val checker1 = RowLevelCheck()
            .isInRange("PULocationID", BigDecimal.valueOf(1), BigDecimal.valueOf(263))
        val verificationResult = VerificationSuite().onDataStream(stream, env.config)
            .addRowLevelCheck(checker1)
            .build()

    fun getRes() : List<RowLevelCheckResult<String>>
    {
        return TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, checker1, 10)
    }
}
