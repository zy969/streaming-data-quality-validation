package consumer

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.logging.log4j.LogManager

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
    var PULocationID:Int ? = 0,
    var DOLocationID:Int ? = 0
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
        val env = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val tempoStream = env.fromElements(t_Data)
        logger.info("~~~~~~~~~~~~env & steam newed")
        val  rowLevelCheck = com.stefan_grafberger.streamdq.checks.row.RowLevelCheck()
            //.isInRange("PULocationID", BigDecimal.valueOf(1), BigDecimal.valueOf(263))
            //.isInRange("DOLocationID", BigDecimal.valueOf(1), BigDecimal.valueOf(263))
            //.isComplete("store_and_fwd_flag")
            /*.isComplete("lpep_pickup_datetime")
            .isComplete("RateCodeID")
            .isComplete("passenger_count")
            .isComplete("trip_distance")
            .isComplete("fare_amount")
            .isComplete("extra")
            .isComplete("mta_tax")
            .isComplete("tip_amount")
            .isComplete("tolls_amount")
            .isComplete("ehail_fee")
            .isComplete("improvement_surcharge")
            .isComplete("total_amount")
            .isComplete("payment_type")
            .isComplete("trip_type")
            .isComplete("congestion_surcharge")
            .isComplete("VendorID")
            .isComplete("PULocationID")
            .isComplete("DOLocationID")*/
        logger.info("check made")

        val verificationResult = VerificationSuite().onDataStream(tempoStream, env.config)
            .addRowLevelCheck(rowLevelCheck)
            .build()
        logger.info("~~~~~~~~~~~~~~veri build done")
        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 10)

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
            PULocationID = data.getOrNull(18)?.toIntOrNull() ?: 0,
            DOLocationID = data.getOrNull(19)?.toIntOrNull() ?: 0
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
