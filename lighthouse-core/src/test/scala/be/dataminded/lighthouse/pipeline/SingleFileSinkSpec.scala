package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.testing.SharedSparkSession
import better.files._
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

class SingleFileSinkSpec extends FunSpec with SharedSparkSession with Matchers with BeforeAndAfterEach {

  import spark.implicits._

  describe("SingleFileSink") {

    it("should write a DataFrame into a single partition") {
      val data = Seq("datadata", "datadatadata").toDF("single").repartition(2)

      SparkFunction.of(data).makeSnapshot(SingleFileSink(TextSink("./target/output/text"))).run(spark)

      ("target" / "output" / "text").glob("*.txt").size should equal(1)
    }
  }

  override protected def afterEach(): Unit = ("target" / "output" / "text").delete(true)
}
