package consumer

import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.logging.log4j.LogManager
import java.math.BigDecimal

data class TaxiRideData @JvmOverloads constructor(
    var store_and_fwd_flag: String? = "",
    var lpep_pickup_datetime:Long? = 0L,
    var lpep_dropoff_datetime:Long? = 0L,
    var RateCodeID: Double ? = 0.0,
    var passenger_count: Double ? = 0.0,
    var trip_distance: Double ? = 0.0,
    var fare_amount: Double ? = 0.0,
    var extra: Double ? = 0.0,
    var mta_tax: Double ? = 0.0,
    var tip_amount: Double ? = 0.0,
    var tolls_amount: Double ? = 0.0,
    var ehail_fee: Double ? = 0.0,
    var improvement_surcharge: Double ? = 0.0,
    var total_amount: Double ? = 0.0,
    var payment_type: Double ? = 0.0,
    var trip_type: Double ? = 0.0,
    var congestion_surcharge: Double ? = 0.0,
    var VendorID: Int ? = 0,
    var PULocationID:Long ? = 0,
    var DOLocationID:Long ? = 0
)

class Validation(var value:String){
    var logger = LogManager.getLogger(Validation::class.java)
    var t_Data: TaxiRideData? = null
    init {
        logger.info("~~~~~~~Contruct entered")
        val cleanedData = value.replace("[{}\"]".toRegex(), "")
        val parts = cleanedData.split(",")
        val eData = parts.mapNotNull { part ->
            val keyValue = part.split(":")
            if (keyValue.size == 2) keyValue[1] else null
        }
        logger.info(eData[0])
        this.t_Data = convertToTaxiRideData(eData)
        logger.info("~~~~~~~Data cleaned")
        val env = StreamExecutionEnvironment.createLocalEnvironment(1)
        val tempoStream = env.fromElements(t_Data)
        logger.info("~~~~~~~~~~~~env & stream newed")
        val  rowLevelCheck = RowLevelCheck()
            .isComplete("PULocationID")
            .isNonNegative("improvement_surcharge")
            .isNonNegative("mta_tax")
            .isNonNegative("tolls_amount")
            .isNonNegative("tip_amount")
            .isContainedIn("VendorID", listOf(1, 2))
            .isComplete("lpep_pickup_datetime")
            .isComplete("lpep_dropoff_datetime")
            .isContainedIn("store_and_fwd_flag", listOf("Y", "N"))
            .isInRange("PULocationID", BigDecimal.valueOf(1L), BigDecimal.valueOf(265L))
            .isInRange("DOLocationID", BigDecimal.valueOf(1L), BigDecimal.valueOf(265L))
            .isInRange("RateCodeID", BigDecimal.valueOf(1.0) , BigDecimal.valueOf(6.0) )
            .isInRange("passenger_count", BigDecimal.valueOf(0.0) , BigDecimal.valueOf(6.0) )
            .isInRange("trip_distance")
            .isInRange("fare_amount")
            .isInRange("total_amount")
            .onTimeOrder("lpep_pickup_datetime", "lpep_dropoff_datetime")
            .isTotaled("total_amount","fare_amount", "extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge", "congestion_surcharge")
        logger.info("check made")

        val verificationResult = VerificationSuite()
            .onDataStream(tempoStream, env.config)
            .addRowLevelCheck(rowLevelCheck)
            .build()
        logger.info("~~~~~~~~~~~~~~veri build done")
        val result = Utils.collectRowLevelResultStream(verificationResult, rowLevelCheck)
        logger.info("~~~~~~~~~~~~~~res:"+result.toString())
    }

    fun convertToTaxiRideData(data: List<String>): TaxiRideData {
        return TaxiRideData(
            store_and_fwd_flag = data.getOrNull(0),
            lpep_pickup_datetime = data.getOrNull(1)?.toLongOrNull() ?: 0L,
            lpep_dropoff_datetime = data.getOrNull(2)?.toLongOrNull() ?: 0L,
            RateCodeID = data.getOrNull(3)?.toDoubleOrNull() ?: 0.0,
            passenger_count = data.getOrNull(4)?.toDoubleOrNull() ?: 0.0,
            trip_distance = data.getOrNull(5)?.toDoubleOrNull() ?: 0.0,
            fare_amount = data.getOrNull(6)?.toDoubleOrNull() ?: 0.0,
            extra = data.getOrNull(7)?.toDoubleOrNull() ?: 0.0,
            mta_tax = data.getOrNull(8)?.toDoubleOrNull() ?: 0.0,
            tip_amount = data.getOrNull(9)?.toDoubleOrNull() ?: 0.0,
            tolls_amount = data.getOrNull(10)?.toDoubleOrNull() ?: 0.0,
            ehail_fee = data.getOrNull(11)?.toDoubleOrNull() ?: 0.0,
            improvement_surcharge = data.getOrNull(12)?.toDoubleOrNull() ?: 0.0,
            total_amount = data.getOrNull(13)?.toDoubleOrNull() ?: 0.0,
            payment_type = data.getOrNull(14)?.toDoubleOrNull() ?: 0.0,
            trip_type = data.getOrNull(15)?.toDoubleOrNull() ?: 0.0,
            congestion_surcharge = data.getOrNull(16)?.toDoubleOrNull() ?: 0.0,
            VendorID = data.getOrNull(17)?.toIntOrNull() ?: 0,
            PULocationID = data.getOrNull(18)?.toLongOrNull() ?: 0L,
            DOLocationID = data.getOrNull(19)?.toLongOrNull() ?: 0L
        )
    }

    fun parseDoubleOrNull(str: String): Double? {
        return if (str.equals("null", ignoreCase = true)) null else str.toDoubleOrNull()
    }

    fun parseIntOrNull(str: String): Int {
        return if (str.equals("null", ignoreCase = true)) 0 else str.toIntOrNull() ?: 0
    }

    fun parseLongOrNull(str: String): Long {
        return if (str.equals("null", ignoreCase = true)) 0L else str.toLongOrNull() ?: 0L
    }
    /*fun getRes() : List<RowLevelCheckResult<String>>
    {
        logger.info("```````````````````````````get res start")
        var data_out_of_range: String = "Data_out_of_range"
        var data_invalid_value: String = "Data_invalid_value"

        logger.info("```````````````````````````checker1")
        val checker1 = RowLevelCheck()
            .isInRange("PULocationID", BigDecimal.valueOf(1), BigDecimal.valueOf(263))

        logger.info("```````````````````````````checker1")
        val verificationResult = VerificationSuite().onDataStream(stream, env.config)
            .addRowLevelCheck(checker1)
            .build()
        logger.info("```````````````````````````verification builded")

        logger.info("``````````````````````````````returning");
        return TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, checker1, 10)
    }

     */
}
