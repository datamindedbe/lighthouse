package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.testing.SharedSparkSession
import better.files._
import org.scalatest.{FunSpec, Matchers}

class SingleFileSinkSpec extends FunSpec with SharedSparkSession with Matchers {

  import spark.implicits._

  describe("SingleFileSink") {

    it("should write a DataFrame into a single partition") {
      val data = Seq("datadata", "datadatadata").toDF("single").repartition(2)

      SparkFunction.of(data).makeSnapshot(TextSink("./target/output/text")).run(spark)

      ("target" / "output" / "text").glob("*.txt").size should equal(1)
    }
  }
}
