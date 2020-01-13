package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.testing.SharedSparkSession
import better.files._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PartitionedOrcSinkSpec extends AnyFunSpec with SharedSparkSession with Matchers {

  import spark.implicits._

  describe("PartitionedOrcSink") {
    it("should write the DataFrame partitioned by a sequence of columns") {
      val data = Seq(("Boy", 15), ("Girl", 22), ("Dog", 3)).toDF("name", "age")

      SparkFunction.of(data).write(PartitionedOrcSink("./target/output/orc", Seq("age"))).run(spark)

      ("target" / "output" / "orc").list.filter(_.isDirectory).map(_.name).toList should contain theSameElementsAs Seq(
        "age=22",
        "age=15",
        "age=3"
      )
    }
  }
}
