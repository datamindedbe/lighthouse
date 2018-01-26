package be.dataminded.lighthouse.datalake

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, Dataset}

trait PathBasedDataLinkTemplate extends DataLink {

  val path: LazyConfig[String]

  def snapshotOf(date: LazyConfig[LocalDate]): SnapshotDataLink = new SnapshotDataLink(this, date)

  final override def read(): DataFrame = doRead(path())

  final override def write[T](dataset: Dataset[T]): Unit = doWrite(dataset, path())

  def doRead(path: String): DataFrame

  def doWrite[T](data: Dataset[T], path: String)
}

class SnapshotDataLink(dataLink: PathBasedDataLinkTemplate, val date: LazyConfig[LocalDate])
    extends PathBasedDataLinkTemplate {

  val path: LazyConfig[String] =
    s"${dataLink.path().stripSuffix("/").trim()}/${date().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))}"

  override def doRead(path: String): DataFrame = dataLink.doRead(path)

  override def doWrite[T](data: Dataset[T], path: String): Unit = dataLink.doWrite(data, path)
}
