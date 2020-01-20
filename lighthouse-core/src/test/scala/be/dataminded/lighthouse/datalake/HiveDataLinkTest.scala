package be.dataminded.lighthouse.datalake

import java.io.File
import java.sql.Date
import java.time.LocalDate
import java.time.Month.DECEMBER

import be.dataminded.lighthouse.Models
import be.dataminded.lighthouse.spark.SparkOverwriteBehavior._
import be.dataminded.lighthouse.testing.SparkFunSuite
import better.files._
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class HiveDataLinkTest extends SparkFunSuite with Matchers with BeforeAndAfterEach {
  import spark.implicits._

  private[this] val expectedTable = new Table(
    name = "customer",
    database = "default",
    description = null,
    tableType = "EXTERNAL",
    isTemporary = false
  )

  test("A hive data reference can be used to write a dataset") {
    val dataset = Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDS()
    val ref     = new HiveDataLink(path = "./target/output/orc", database = "default", table = "customer")

    ref.write(dataset)
    val actualTable = spark.catalog.listTables().filter(_.name == "customer").head()

    ref.read().count should equal(1)
    actualTable.toString() shouldBe expectedTable.toString()
  }

  test("A hive datalink can be used in overwrite mode") {
    val ingestDate1 = LocalDate.of(1982, DECEMBER, 21)
    val ingestDate2 = LocalDate.of(1982, DECEMBER, 22)
    val filePath    = "./target/output/orc"
    val rootFile    = new File(filePath)

    val dataset1 = Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982"))
      .toDS()
      .withColumn("ingestDate", lit(Date.valueOf(ingestDate1)))
    val dataset2 = Seq(Models.RawCustomer("2", "Elisabeth", "Moss", "1982"))
      .toDS()
      .withColumn("ingestDate", lit(Date.valueOf(ingestDate2)))
    val ref =
      new HiveDataLink(path = filePath, database = "default", table = "customer", partitionedBy = List("ingestDate"))

    ref.snapshotOf(ingestDate1)
    ref.write(dataset1)
    ref.read().count should equal(1)
    rootFile.listFiles.filter(_.isDirectory).toList should
      (contain(new File(s"$filePath/ingestDate=1982-12-21")) and have length 1)

    ref.write(dataset2)
    ref.read().count should equal(2)
    rootFile.listFiles.filter(_.isDirectory).toList should
      (contain(new File(s"$filePath/ingestDate=1982-12-22")) and have length 2)
  }

  test("A hive data reference can be used to write a dataframe") {
    val dataset = Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDF()
    val ref     = new HiveDataLink(path = "./target/output/orc", database = "default", table = "customer")

    ref.write(dataset)
    val actualTable = spark.catalog.listTables().filter(_.name == "customer").head()

    ref.read().count should equal(1)
    actualTable.toString() shouldBe expectedTable.toString()
  }

  test("Overwrite multiple partitions") {
    val dataset = Seq(
      Models.RawCustomer("1", "Pascal", "Knapen", "1982"),
      Models.RawCustomer("2", "Elisabeth", "Moss", "1982"),
      Models.RawCustomer("3", "Donald", "Glover", "1983")
    ).toDF()
    val updating = Seq(
      Models.RawCustomer("4", "Kate", "Middleton", "1982")
    ).toDF()
    val expected = Seq(
      Models.RawCustomer("4", "Kate", "Middleton", "1982"),
      Models.RawCustomer("3", "Donald", "Glover", "1983")
    )

    val ref = new HiveDataLink(
      path = "./target/output/orc",
      database = "default",
      table = "client",
      partitionedBy = List("yearOfBirth"),
      overwriteBehavior = MultiplePartitionOverwrite
    )

    ref.write(dataset)
    ref.read().count should equal(3)

    ref.write(updating)
    ref.read().count should equal(2)

    val actualTable = spark.catalog.listTables().filter(_.name == "customer").head()

    ref.readAs[Models.RawCustomer]().collect() should contain theSameElementsAs expected
    actualTable.toString() shouldBe expectedTable.toString()
  }

  override def beforeEach(): Unit = {
    ("target" / "output").delete(swallowIOExceptions = true)
  }
}
