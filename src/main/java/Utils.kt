package consumer

import com.stefan_grafberger.streamdq.VerificationResult
import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck

object Utils {
    fun <IN> collectRowLevelResultStream(
        verificationResult: VerificationResult<IN, *>,
        rowLevelCheck: RowLevelCheck,
    ): List<RowLevelCheckResult<IN>> {
        val checkResultStream = verificationResult.getResultsForCheck(rowLevelCheck)
        val resultList = checkResultStream!!.executeAndCollect().asSequence().toList()
        return resultList
    }
}