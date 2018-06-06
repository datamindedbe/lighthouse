package be.dataminded.lighthouse.datalake

import java.time.{LocalDate, Month}

import org.scalatest.{FunSuite, Matchers}

class DatalakeTest extends FunSuite with Matchers {

  val datalake = new SampleDatalake()

  test("A datalake uses the `test` environment by default") {
    val dataRef = datalake.getDataLink(datalake.uid)

    dataRef should equal(datalake.testRef)
  }

  test("A datalake can retrieve properties through its apply method") {
    val dataRef = datalake(datalake.uid)

    dataRef should equal(datalake.testRef)
  }

  test("System property allows you to change environment") {
    System.setProperty(Datalake.PropertyName, "acc")
    val datalake = new SampleDatalake()

    val dataRef = datalake.getDataLink(datalake.uid)

    dataRef should equal(datalake.accRef)
  }

  test("Once a reference is retrieved the environment cannot be changed") {
    val testRef = datalake.getDataLink(datalake.uid)
    System.getProperty(Datalake.PropertyName, "acc")
    val accRef = datalake.getDataLink(datalake.uid)

    testRef should equal(accRef)
  }

  test("Exception is thrown when requesting an unknown reference") {
    an[NoSuchElementException] should be thrownBy datalake.getDataLink(DataUID("datamined", "unknown_key"))
  }

  test("Default Execution date is set to today") {
    val dataRef = datalake.getDataLink(datalake.snapshotUID).asInstanceOf[SnapshotDataLink]

    dataRef.date() should equal(LocalDate.now())
  }

  test("Execution date can be set to be used with snapshots") {
    val date      = LocalDate.of(2017, Month.JANUARY, 1)
    val warehouse = new SampleDatalake(date)
    val dataRef   = warehouse.getDataLink(warehouse.snapshotUID).asInstanceOf[SnapshotDataLink]

    dataRef.date() should equal(date)
  }
}

/**
  * Use a class here to make the tests run in isolation
  */
class SampleDatalake(executionDate: LocalDate = LocalDate.now()) extends Datalake {

  val uid         = DataUID("datamined", "key")
  val snapshotUID = DataUID("datamined", "snapshotkey")

  val testRef = new HiveDataLink("s3://test", table = "test", database = "test")
  val accRef  = new HiveDataLink("s3://acc", table = "acc", database = "acc")

  val testSnapshotRef: SnapshotDataLink =
    new HiveDataLink("s3://test", table = "test", database = "test").snapshotOf(executionDate)
  val accSnapshotRef: SnapshotDataLink =
    new HiveDataLink("s3://test", table = "test", database = "test").snapshotOf(executionDate)

  environment(Datalake.DefaultEnvironment) { references =>
    references += (uid         -> testRef)
    references += (snapshotUID -> testSnapshotRef)
  }

  environment("acc") { references =>
    references += (uid         -> accRef)
    references += (snapshotUID -> accSnapshotRef)
  }
}
