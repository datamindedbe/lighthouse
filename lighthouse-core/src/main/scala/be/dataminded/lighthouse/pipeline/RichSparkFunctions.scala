package be.dataminded.lighthouse.pipeline

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object RichSparkFunctions extends LazyLogging {

  class DatasetSparkFunction[A <: Dataset[_]: ClassTag](function: SparkFunction[A]) {

    /*
     * Print schema originally returns Unit, wrapping it in the SparkFunction allows you to chain the method
     */
    def printSchema(): SparkFunction[A] =
      function.map { dataSet =>
        dataSet.printSchema()
        dataSet
      }

    def as[T: Encoder]: SparkFunction[Dataset[T]] = function.map(_.as[T])

    def cache(storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SparkFunction[A] =
      function.map {
        _.persist(storageLevel)
      }

    def dropCache(): SparkFunction[A] =
      function.map {
        _.unpersist()
      }

    def write(sink: Sink, sinks: Sink*): SparkFunction[A] = {
      if (sinks.isEmpty) function.map { data =>
        sink.write(data); data
      }
      else (sink +: sinks).foldLeft(function.cache())((f, sink) => f.write(sink))
    }

    def count(): SparkFunction[Long] = {
      function.map { dataSet =>
        val n = dataSet.count()
        logger.debug(s"The data set produced $n rows")
        n
      }
    }
  }
}
