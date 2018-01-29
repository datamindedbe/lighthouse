package be.dataminded.lighthouse.testing

import be.dataminded.lighthouse.testing.DataFramePrettyPrinter.prettyPrintDataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.Suite

trait DatasetComparer extends DatasetComparerLike {
  self: Suite =>
}

sealed trait DatasetComparerLike {

  def assertDatasetEquality[T](actual: Dataset[T], expected: Dataset[T], orderedComparison: Boolean = true): Unit = {

    def simplifySchema(dataset: Dataset[T]) = dataset.schema.map(field => (field.name, field.dataType))

    assert(simplifySchema(actual) == simplifySchema(expected), schemaMismatchMessage(actual, expected))

    def defaultSortedDataset(ds: Dataset[T]): Dataset[T] = {
      ds.sort(ds.columns.sorted.map(col): _*)
    }

    if (orderedComparison) {
      assert(actual.collect().sameElements(expected.collect()), contentMismatchMessage(actual, expected))
    } else {
      val sortedActual   = defaultSortedDataset(actual)
      val sortedExpected = defaultSortedDataset(expected)
      assert(sortedActual.collect().sameElements(sortedExpected.collect()),
             contentMismatchMessage(sortedActual, sortedExpected))
    }
  }

  private def schemaMismatchMessage[T](actual: Dataset[T], expected: Dataset[T]): String =
    s"""
       |Actual schema:
       |${actual.schema}
       |Expected schema:
       |${expected.schema}
     """.stripMargin

  private def contentMismatchMessage[T](actual: Dataset[T], expected: Dataset[T]): String =
    s"""
       |Actual content:
       |${prettyPrintDataFrame(actual.toDF(), 5)}
       |Expected DataFrame Content:
       |${prettyPrintDataFrame(expected.toDF(), 5)}
     """.stripMargin
}
