package be.dataminded.lighthouse.testing

import org.scalatest.Matchers

class SparkFunSuiteTest extends SparkFunSuite with Matchers {

  sparkTest("A SparkTest has an instance of SparkSession available") {
    assertCompiles("spark")
  }

  sparkIntegrationTest("A SparkIntegrationTest has an instance of SparkSession available") {
    assertCompiles("spark")
  }
}
