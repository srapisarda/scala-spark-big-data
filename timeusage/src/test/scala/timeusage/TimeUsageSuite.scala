package timeusage

import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}


@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {


  val resource = TimeUsage.fsPath("/timeusage/atussum.csv")


  val rdd = TimeUsage.spark.sparkContext.textFile(resource)


  test("'dfSchema' should return a defined schema as StructType ") {

    val headerColumns = rdd.first().split(",").to[List]
    val dfSchema: StructType = TimeUsage.dfSchema(headerColumns)

    assert( dfSchema != null )
    assert( dfSchema.size ==  headerColumns.length )
  }

}
