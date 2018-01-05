package be.dataminded.candela.testing

import be.dataminded.lighthouse.testing.SparkFunSuite
import org.scalatest.Matchers

class SparkFunSuiteTest extends SparkFunSuite with Matchers {

  import spark.implicits._

  sparkTest("A SparkFunSuite has an instance of SparkSession available") {
    assertCompiles("spark")
  }

  sparkTest("A SparkFunSuite has a SqlContext available") {
    assertCompiles("sqlContext")
  }

  sparkTest("A SparkFunSuite can check the equality of two DataFrames") {
    val expected = Seq(
      ("Egbert", "Zwartbroek"),
      ("Jodokus", "Kindermans")
    ).toDF("first_name", "last_name")

    val actual = Seq(
      ("Egbert", "Zwartbroek"),
      ("Jodokus", "Kindermans")
    ).toDF("first_name", "last_name")

    assertDataFrameEquals(expected, actual)
  }

  sparkTest("A SparkFunSuite can check partial equality of two DataFrames") {
    val expected = Seq(
      ("Egbert", "Zwartbroek", 10.21),
      ("Jodokus", "Kindermans", 33.12)
    ).toDF("first_name", "last_name", "ratio")

    val actual = Seq(
      ("Egbert", "Zwartbroek", 10.00),
      ("Jodokus", "Kindermans", 33.10)
    ).toDF("first_name", "last_name", "ratio")

    assertDataFrameApproximateEquals(expected, actual, 0.25)
  }

  sparkTest("A SparkFunSuite can check the equality of two DataSets") {
    val expected = Seq(
      Person("Egbert", "Zwartbroek", 10.21),
      Person("Jodokus", "Kindermans", 33.12)
    ).toDS()

    val actual = Seq(
      Person("Egbert", "Zwartbroek", 10.21),
      Person("Jodokus", "Kindermans", 33.12)
    ).toDS()

    assertDatasetEquals(expected, actual)
  }

  sparkTest("A SparkFunSuite can check partial equality of two DataSets") {
    val expected = Seq(
      Person("Egbert", "Zwartbroek", 10.21),
      Person("Jodokus", "Kindermans", 33.12)
    ).toDS()

    val actual = Seq(
      Person("Egbert", "Zwartbroek", 10.00),
      Person("Jodokus", "Kindermans", 33.10)
    ).toDS()

    assertDatasetApproximateEquals(expected, actual, 0.25)
  }
}

case class Person(firstName: String, lastName: String, ratio: Double)
