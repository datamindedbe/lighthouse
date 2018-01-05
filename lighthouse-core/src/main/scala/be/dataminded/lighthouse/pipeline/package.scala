package be.dataminded.lighthouse

import be.dataminded.lighthouse.pipeline.RichSparkFunctions.DatasetSparkFunction
import org.apache.spark.sql.Dataset

import scala.language.implicitConversions
import scala.reflect.ClassTag

package object pipeline {

  implicit def toDatasetSparkFunction[A <: Dataset[_]: ClassTag](operation: SparkFunction[A]): DatasetSparkFunction[A] =
    new DatasetSparkFunction(operation)

}
