package be.dataminded.lighthouse.datalake

import java.time.{LocalDate, Month}

import org.scalatest.{FunSuite, Matchers}

class DatalakeTest extends FunSuite with Matchers {

  test("A warehouse uses the `test` environment by default") {
    val warehouse = new SampleDatalake()
    val dataRef   = warehouse.getDataLink(warehouse.UID)

    dataRef should equal(warehouse.testRef)
  }

  test("System property allows you to change environment") {
    val warehouse = new SampleDatalake()
    System.setProperty(Datalake.SYSTEM_PROPERTY, "acc")
    val dataRef = warehouse.getDataLink(warehouse.UID)

    dataRef should equal(warehouse.accRef)
  }

  test("Once a reference is retrieved the environment cannot be changed") {
    val warehouse = new SampleDatalake()
    val testRef   = warehouse.getDataLink(warehouse.UID)
    System.getProperty(Datalake.SYSTEM_PROPERTY, "acc")
    val accRef = warehouse.getDataLink(warehouse.UID)

    testRef should equal(accRef)
  }

  test("Exception is thrown when requesting an unknown reference") {
    val warehouse = new SampleDatalake()
    an[NoSuchElementException] should be thrownBy warehouse.getDataLink(DataUID("datamined", "unknown_key"))
  }

  test("Default Execution date is set to today") {
    val warehouse = new SampleDatalake()
    val dataRef   = warehouse.getDataLink(warehouse.snapshotUID).asInstanceOf[SnapshotDataLink]

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

  val UID         = DataUID("datamined", "key")
  val snapshotUID = DataUID("datamined", "snapshotkey")

  val testRef = new HiveDataLink("s3://test", table = "test", database = "test")
  val accRef  = new HiveDataLink("s3://acc", table = "acc", database = "acc")

  val testSnapshotRef: SnapshotDataLink =
    new HiveDataLink("s3://test", table = "test", database = "test").snapshotOf(executionDate)
  val accSnapshotRef: SnapshotDataLink =
    new HiveDataLink("s3://test", table = "test", database = "test").snapshotOf(executionDate)

  environment(Datalake.DEFAULT_ENVIRONMENT) { references =>
    references += (UID         -> testRef)
    references += (snapshotUID -> testSnapshotRef)
  }

  environment("acc") { references =>
    references += (UID         -> accRef)
    references += (snapshotUID -> accSnapshotRef)
  }
}
