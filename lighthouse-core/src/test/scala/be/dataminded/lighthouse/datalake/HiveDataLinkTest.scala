package be.dataminded.lighthouse.datalake

import java.time.LocalDate
import java.time.Month.DECEMBER

import be.dataminded.lighthouse.Models
import be.dataminded.lighthouse.spark.SparkOverwriteBehavior._
import be.dataminded.lighthouse.testing.SparkFunSuite
import better.files._
import org.apache.spark.sql.SaveMode
import org.scalatest.{BeforeAndAfterEach, Matchers}

class HiveDataLinkTest extends SparkFunSuite with Matchers with BeforeAndAfterEach {

  val customerPath: String = File.resource("customers.csv").pathAsString
  val ordersPath: String   = File.resource("orders.csv").pathAsString
  val options              = Map("header" -> "true")

  test("A hive data reference can be used to write a dataset") {
    import spark.implicits._
    val dataset = Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDS()
    val ref     = new HiveDataLink(path = "./target/output/orc", database = "default", table = "customer")
    ref.write(dataset)

    ref.readAs[Models.RawCustomer]().count should equal(1)
    //TODO: Result is correct, matcher fails: investigate
    //spark.catalog.listTables().collect() should contain (new org.apache.spark.sql.catalog.Table(
    //name = "customer", database = "default", description = null, tableType = "EXTERNAL", isTemporary = false))
  }

  test("A hive datalink can be used in overwrite mode") {

    import spark.implicits._
    val filePath = "./target/output/orc"
    import org.apache.spark.sql.functions._
    val saveMode    = SaveMode.Overwrite
    val ingestDate1 = LocalDate.of(1982, DECEMBER, 21)
    val dataset1 = Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982"))
      .toDS()
      .withColumn("ingestDate", lit(java.sql.Date.valueOf(ingestDate1)))
    val ref =
      new HiveDataLink(path = filePath, database = "default", table = "customer", partitionedBy = List("ingestDate"))
    ref.snapshotOf(LocalDate.of(1982, DECEMBER, 21))
    ref.write(dataset1)
    ref.readAs[Models.RawCustomer]().count should equal(1)
    val rootFile = new java.io.File(filePath)
    rootFile.listFiles.filter(_.isDirectory).toList should
      (contain(new java.io.File(s"$filePath/ingestDate=1982-12-21")) and have length 1)

    val ingestDate2 = LocalDate.of(1982, DECEMBER, 22)
    val dataset2 = Seq(Models.RawCustomer("2", "Elisabeth", "Moss", "1982"))
      .toDS()
      .withColumn("ingestDate", lit(java.sql.Date.valueOf(ingestDate2)))

    ref.write(dataset2)
    ref.readAs[Models.RawCustomer]().count should equal(2)
    rootFile.listFiles.filter(_.isDirectory).toList should
      (contain(new java.io.File(s"$filePath/ingestDate=1982-12-22")) and have length 2)
  }

  test("A hive data reference can be used to write a dataframe") {
    import spark.implicits._
    val dataset = Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDF()
    val ref     = new HiveDataLink(path = "./target/output/orc", database = "default", table = "customer")
    ref.write(dataset)

    ref.read().count should equal(1)
    //TODO: Result is correct, matcher fails: investigate
    //spark.catalog.listTables().collect() should contain (new org.apache.spark.sql.catalog.Table(
    //name = "customer", database = "default", description = null, tableType = "EXTERNAL", isTemporary = false))
  }

  test("Overwrite multiple partitions") {
    import spark.implicits._
    val dataset = Seq(
      Models.RawCustomer("1", "Pascal", "Knapen", "1982"),
      Models.RawCustomer("2", "Elisabeth", "Moss", "1982"),
      Models.RawCustomer("3", "Donald", "Glover", "1983")
    ).toDF()

    val ref = new HiveDataLink(
      path = "./target/output/orc",
      database = "default",
      table = "client",
      partitionedBy = List("yearOfBirth"),
      overwriteBehavior = MultiplePartitionOverwrite
    )

    ref.write(dataset)
    ref.readAs[Models.RawCustomer]().count should equal(3)

    val updating = Seq(
      Models.RawCustomer("4", "Kate", "Middleton", "1982")
    ).toDF()

    ref.write(updating)
    ref.readAs[Models.RawCustomer]().count should equal(2)

    val expected = Seq(
      Models.RawCustomer("4", "Kate", "Middleton", "1982"),
      Models.RawCustomer("3", "Donald", "Glover", "1983")
    )

    ref.readAs[Models.RawCustomer]().collect() should contain theSameElementsAs expected

    //TODO: Result is correct, matcher fails: investigate
    //spark.catalog.listTables().collect() should contain (new org.apache.spark.sql.catalog.Table(
    //name = "customer", database = "default", description = null, tableType = "EXTERNAL", isTemporary = false))
  }

  override def beforeEach(): Unit = {
    ("target" / "output").delete(swallowIOExceptions = true)
  }
}
