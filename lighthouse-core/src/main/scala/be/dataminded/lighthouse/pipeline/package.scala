package be.dataminded.lighthouse

import be.dataminded.lighthouse.datalake.DataLink
import be.dataminded.lighthouse.pipeline.RichSparkFunctions.DatasetSparkFunction
import cats.syntax.TupleSemigroupalSyntax
import org.apache.spark.sql.Dataset

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Some implicit extensions to work easier with the [[be.dataminded.lighthouse.pipeline.SparkFunction]].
  *
  * Also expose the [[TupleSemigroupalSyntax]] of Cats so users can use `mapN`, `tupled`, etc on the SparkFunction
  * without importing `cats.implicits._` themselves
  */
package object pipeline extends TupleSemigroupalSyntax {

  implicit def toDatasetSparkFunction[A <: Dataset[_]: ClassTag](operation: SparkFunction[A]): DatasetSparkFunction[A] =
    new DatasetSparkFunction(operation)

  implicit def easyDataLinkSink(dataLink: DataLink): DataLinkSink = DataLinkSink(dataLink)
}
