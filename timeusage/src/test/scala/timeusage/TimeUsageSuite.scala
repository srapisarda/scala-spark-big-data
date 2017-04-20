package timeusage

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}


@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  val resource: String = TimeUsage.fsPath("/timeusage/atussum.csv")
  val rdd: RDD[String] = TimeUsage.spark.sparkContext.textFile(resource)

  test("'dfSchema' should return a defined schema as StructType ") {

    val headerColumns = rdd.first().split(",").to[List]
    val dfSchema: StructType = TimeUsage.dfSchema(headerColumns)

    assert(dfSchema != null)
    assert(dfSchema.size == headerColumns.length)
  }

  test("'classifiedColumns' should return a defined triple of df Columns ") {
    val headerColumns = rdd.first().split(",").to[List]
    val (t1, t2, t3) = TimeUsage.classifiedColumns(headerColumns)
    assert(t1.nonEmpty)
    assert(t2.nonEmpty)
    assert(t3.nonEmpty)
  }

  test("'timeUsageSummary' should return a data frame defined"){
    val (columns, initDf) = TimeUsage.read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)
    val summaryDf: DataFrame = TimeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    assert(summaryDf != null)
  }

}
