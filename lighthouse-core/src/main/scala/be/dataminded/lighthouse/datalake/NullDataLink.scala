package be.dataminded.lighthouse.datalake

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

/**
  * DataLink implementation with no effect for testing purposes
  */
class NullDataLink extends DataLink {
  override def readAs[T: Encoder](): Dataset[T] = spark.createDataset(Seq[T]())

  def read(): DataFrame = spark.createDataFrame(Seq())

  def write[T](dataset: Dataset[T]): Unit = ()
}
