package be.dataminded.lighthouse.datalake

import be.dataminded.lighthouse.spark.SparkSessions
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

/**
  * A reference to data with the functionality to read and write
  */
trait DataLink extends SparkSessions {

  def readAs[T: Encoder](): Dataset[T] = read().as[T]

  def read(): DataFrame

  def write[T](dataset: Dataset[T]): Unit
}
