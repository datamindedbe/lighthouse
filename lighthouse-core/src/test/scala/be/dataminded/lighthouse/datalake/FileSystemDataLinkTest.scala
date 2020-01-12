package be.dataminded.lighthouse.datalake

import java.time.LocalDate
import java.time.Month.DECEMBER

import be.dataminded.lighthouse.Models
import be.dataminded.lighthouse.spark.Csv
import be.dataminded.lighthouse.testing.SparkFunSuite
import better.files._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, Matchers}

class FileSystemDataLinkTest extends SparkFunSuite with Matchers with BeforeAndAfter {

  import spark.implicits._

  val customerPath: String = File.resource("customers.csv").pathAsString
  val ordersPath: String   = File.resource("orders.csv").pathAsString
  val options              = Map("header" -> "true")

  test("A FileSystemDataLink can read a DataFrame from a local file") {
    val link = new FileSystemDataLink(path = customerPath, format = Csv, options = options)
    link.read().count should equal(3)
  }

  test("A FileSystemDataLink can read a DataSet from a local file") {
    val link    = new FileSystemDataLink(path = customerPath, format = Csv, options = options)
    val dataset = link.readAs[Models.RawCustomer]()

    dataset.count should equal(3)
  }

  test("A FileSystemDataLink can leverage a specified schema") {
    val schema = Option(
      StructType(
        StructField("id", ByteType) ::
          StructField("firstName", StringType, nullable = true) ::
          StructField("lastName", StringType, nullable = true) ::
          StructField("yearOfBirth", ShortType, nullable = true) :: Nil
      )
    )
    val link    = new FileSystemDataLink(path = customerPath, format = Csv, options = options, schema = schema)
    val dataset = link.read()

    dataset.schema should equal(schema.get)
  }

  test("A FileSystemDataLink can be used to write a DataFrame") {
    val link = new FileSystemDataLink(path = "./target/output/orc")
    link.write(Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDF())

    link.read().count should equal(1)
  }

  test("A FileSystemDataLink can be used to write a DataSet") {
    val link = new FileSystemDataLink(path = "./target/output/orc")
    link.write(Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDS())

    link.read().count should equal(1)
  }

  test("A snapshot of FileSystemDataLink can be used to write a DataFrame for a specific date") {
    val link = new FileSystemDataLink(path = "./target/output/orc").snapshotOf(LocalDate.of(1982, DECEMBER, 21))
    link.write(Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDF())

    "./target/output/orc/1982/12/21".toFile.exists should equal(true)
    link.read().count should equal(1)
  }

  test("A snapshot of FileSystemDataLink can be used to write a DataSet for a specific date") {
    val link = new FileSystemDataLink(path = "./target/output/orc").snapshotOf(LocalDate.of(1982, DECEMBER, 21))
    link.write(Seq(Models.RawCustomer("1", "Pascal", "Knapen", "1982")).toDS())

    "./target/output/orc/1982/12/21".toFile.exists should equal(true)
    link.read().count should equal(1)
  }

  after {
    file"target/output".delete(true)
  }
}
