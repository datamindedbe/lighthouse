package be.dataminded.lighthouse.demo

import be.dataminded.lighthouse.testing.{DatasetComparer, SparkFunSuite}
import better.files._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

class AirplanePipelineSpec extends SparkFunSuite with AirplanePipeline with Matchers with DatasetComparer {

  import spark.implicits._

  val result: DataFrame = pipeline.run(spark)

  test("The pipeline did run correctly and produces output files") {
    file"target/clean/airplane".glob("*.orc").length should be(1)
    file"target/clean/weather/daily".glob("*.orc").length should be(1)
    file"target/clean/weather/stations".glob("*.orc").length should be(1)
    file"target/master/airplane/view".glob("*.orc").length should not be 0
  }

  test("The airplane master view is available in Hive") {
    spark.table("airplane_view").show(5)
  }

  test("Smaller portions of the pipeline can also be executed for easier testing") {
    val cleanWeatherStations = weatherStations.run(spark)

    file"target/clean/weather/stations".glob("*.orc").length should be(1)
    cleanWeatherStations.schema should equal(
      StructType(
        StructField("WBAN", IntegerType) :: StructField("IAT", StringType) :: Nil
      )
    )
  }

  test("Small functions can be tested easier") {
    val weatherStations = Seq(
      (3011, "TEX", 37.954, -107.901),
      (3012, "SKX", 36.458, -105.667),
      (3013, "LAA", 38.070, -102.688)
    ).toDF("WBAN", "CallSign", "Latitude", "Longitude")

    val result = cleanWeatherStations(weatherStations).run(spark)

    result.schema should equal(
      StructType(
        StructField("WBAN", IntegerType, nullable = false) :: StructField("IAT", StringType) :: Nil
      )
    )
  }

  test("We can easily test for content using one of the included DatasetComparer") {
    val weatherStations = Seq(
      (3011, "TEX", 37.954, -107.901),
      (3012, "SKX", 36.458, -105.667),
      (3013, "LAA", 38.070, -102.688)
    ).toDF("WBAN", "CallSign", "Latitude", "Longitude")

    val result = cleanWeatherStations(weatherStations).run(spark)

    val expectedStations = Seq(
      (3011, "TEX"),
      (3012, "SKX"),
      (3013, "LAA")
    ).toDF("WBAN", "IAT")

    assertDatasetEquality(result, expectedStations, orderedComparison = false)
  }
}
