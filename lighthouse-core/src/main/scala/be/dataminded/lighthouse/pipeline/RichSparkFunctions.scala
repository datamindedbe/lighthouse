package be.dataminded.lighthouse.pipeline

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

object RichSparkFunctions extends LazyLogging {

  class DatasetSparkFunction[A <: Dataset[_]: ClassTag](sparkFunction: SparkFunction[A]) {

    /*
     * Print schema originally returns Unit, wrapping it in the SparkFunction allows you to chain the method
     */
    def printSchema(): SparkFunction[A] = sparkFunction.map { dataSet =>
      dataSet.printSchema()
      dataSet
    }

    def as[T: Encoder]: SparkFunction[Dataset[T]] = sparkFunction.map(_.as[T])

    def cache(storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SparkFunction[A] = sparkFunction.map {
      _.persist(storageLevel)
    }

    def dropCache(): SparkFunction[A] = sparkFunction.map {
      _.unpersist()
    }

    def makeSnapshot(sink: Sink): SparkFunction[A] = sparkFunction.map { data =>
      sink.write(data)
      data
    }

    def makeSnapshots(sinks: Sink*): SparkFunction[A] = {
      sinks.foldLeft(sparkFunction.cache())((f, sink) => f.makeSnapshot(sink))
    }

    def count(): SparkFunction[Long] = {
      sparkFunction.map { dataSet =>
        val n = dataSet.count()
        logger.debug(s"The data set produced $n rows")
        n
      }
    }
  }
}
