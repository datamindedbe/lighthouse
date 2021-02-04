package be.dataminded.lighthouse.datalake

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, Dataset}
import be.dataminded.lighthouse.common.DateTimeFormatters

class SnapshotDataLink(dataLink: PathBasedDataLink, val date: LazyConfig[LocalDate], val pathSuffix: Option[String])
    extends PathBasedDataLink {
  val path: LazyConfig[String] =
    s"${dataLink.path().trim().stripSuffix("/")}/${date().format(DateTimeFormatters.SimpleDateFormat)}/${pathSuffix
      .getOrElse("")}"

  override def doRead(path: String): DataFrame = dataLink.doRead(path)

  override def doWrite[T](data: Dataset[T], path: String): Unit = dataLink.doWrite(data, path)
}
