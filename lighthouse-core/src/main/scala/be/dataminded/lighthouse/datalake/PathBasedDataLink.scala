package be.dataminded.lighthouse.datalake

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, Dataset}

trait PathBasedDataLink extends DataLink {
  val path: LazyConfig[String]

  def snapshotOf(date: LazyConfig[LocalDate], pathSuffix: String = ""): SnapshotDataLink =
    new SnapshotDataLink(this, date, pathSuffix)

  def partitionOf(partition: LazyConfig[String]): PartitionedDataLink = new PartitionedDataLink(this, partition)

  final override def read(): DataFrame = doRead(path())

  final override def write[T](dataset: Dataset[T]): Unit = doWrite(dataset, path())

  def doRead(path: String): DataFrame

  def doWrite[T](data: Dataset[T], path: String)
}
