package be.dataminded.lighthouse.pipeline

import java.io.ByteArrayOutputStream

import be.dataminded.lighthouse.testing.SharedSparkSession
import better.files._
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

class RichSparkFunctionsSpec extends FunSpec with Matchers with SharedSparkSession with BeforeAndAfter {

  import spark.implicits._

  describe("SparkFunctions with a DataSet inside should have extra functionality") {

    val function = SparkFunction.of(Seq(1, 2, 3, 4, 5).toDS())

    it("can cache") {
      function.cache().run(spark).storageLevel should equal(StorageLevel.MEMORY_ONLY)
    }

    it("can drop the cache") {
      function.cache().dropCache().run(spark).storageLevel should equal(StorageLevel.NONE)
    }

    it("can be written to a sink") {
      function.makeSnapshot(OrcSink("target/output/orc")).run(spark)

      file"target/output/orc".exists should be(true)
    }

    it("can be written to multiple sinks") {
      function.makeSnapshots(OrcSink("target/output/orc"), OrcSink("target/output/orc2")).run(spark)

      file"target/output/orc".exists should be(true)
      file"target/output/orc2".exists should be(true)
    }

    it("is being cached when writing to multiple sinks for performance") {
      val result = function.makeSnapshots(OrcSink("target/output/orc"), OrcSink("target/output/orc2")).run(spark)

      result.storageLevel should equal(StorageLevel.MEMORY_ONLY)
    }

    it("can easily be counted") {
      function.count().run(spark) should equal(5)
    }

    it("can have columns renamed") {
      function.renameColumns(Map("value" -> "new_value")).run(spark).columns should equal(Array("new_value"))
    }

    it("can print the schema") {
      val stream = new ByteArrayOutputStream()
      Console.withOut(stream) {
        function.printSchema().run(spark)
      }
      stream.toString() should include("value: integer (nullable = false)")
    }

    it("can be be used as a Dataset") {
      function.as[Int].run(spark) shouldBe a[Dataset[Int]]
    }
  }

  after {
    file"target/output/orc".delete(true)
    file"target/output/orc2".delete(true)
  }
}
