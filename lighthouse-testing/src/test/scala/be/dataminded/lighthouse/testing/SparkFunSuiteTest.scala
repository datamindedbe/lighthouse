package be.dataminded.lighthouse.testing

import org.scalatest.Matchers

class SparkFunSuiteTest extends SparkFunSuite with Matchers {

  sparkTest("A SparkFunSuite has an instance of SparkSession available") {
    assertCompiles("spark")
  }
}
