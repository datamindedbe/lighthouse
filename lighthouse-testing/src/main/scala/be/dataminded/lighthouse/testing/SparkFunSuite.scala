package be.dataminded.lighthouse.testing

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Tag}

case object SparkTest            extends Tag("be.dataminded.lighthouse.testing.SparkTest")
case object SparkIntegrationTest extends Tag("be.dataminded.lighthouse.testing.SparkIntegrationTest")

/**
  * Base class for testing Spark-based applications.
  */
abstract class SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll with SharedSparkSession {

  def test(name: String)(body: => Any /* Assertion */ ): Unit = {
    test(name, SparkTest) {
      body
    }
  }

  override def afterAll(): Unit = spark.catalog.clearCache()
}

abstract class SparkIntegrationFunSuite extends AnyFunSuite with BeforeAndAfterAll with SharedSparkSession {

  def test(name: String)(body: => Any /* Assertion */ ): Unit = {
    test(name, SparkIntegrationTest) {
      body
    }
  }

  override def afterAll(): Unit = spark.catalog.clearCache()
}
