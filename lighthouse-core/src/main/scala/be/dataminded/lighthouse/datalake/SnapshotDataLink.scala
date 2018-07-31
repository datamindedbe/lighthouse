package be.dataminded.lighthouse.datalake

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, Dataset}

class SnapshotDataLink(dataLink: PathBasedDataLink, val date: LazyConfig[LocalDate], val pathSuffix: String) extends PathBasedDataLink {
  val path: LazyConfig[String] =
    s"${dataLink.path().trim().stripSuffix("/")}/${date().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))}/${pathSuffix}"

  override def doRead(path: String): DataFrame = dataLink.doRead(path)

  override def doWrite[T](data: Dataset[T], path: String): Unit = dataLink.doWrite(data, path)
}
