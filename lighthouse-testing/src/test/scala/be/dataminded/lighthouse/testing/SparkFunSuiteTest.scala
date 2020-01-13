package be.dataminded.lighthouse.testing

import org.scalatest.matchers.should.Matchers

class SparkFunSuiteTest extends SparkFunSuite with Matchers {

  test("A SparkTest has an instance of SparkSession available") {
    assertCompiles("spark")
  }
}

class SparkIntegartionFunSuiteTest extends SparkIntegrationFunSuite with Matchers {

  test("A SparkIntegrationTest has an instance of SparkSession available") {
    assertCompiles("spark")
  }
}
