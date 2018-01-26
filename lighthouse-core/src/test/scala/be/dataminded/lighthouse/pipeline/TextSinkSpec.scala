package be.dataminded.lighthouse.pipeline

import better.files._
import be.dataminded.lighthouse.testing.SharedSparkSession
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

class TextSinkSpec extends FunSpec with SharedSparkSession with Matchers with BeforeAndAfterEach {

  import spark.implicits._

  describe("TextSink") {
    it("should write the contents of a DataFrame as text") {
      val data = Seq("datadata", "datadatadata").toDF("single")

      SparkFunction.of(data).makeSnapshot(TextSink("./target/output/text")).run(spark)

      ("target" / "output" / "text").glob("*.txt").map(_.contentAsString).toSeq should equal(
        Stream("datadata\n", "datadatadata\n"))
    }
  }

  override protected def afterEach(): Unit = {
    ("target" / "output" / "text").delete(true)
  }
}
