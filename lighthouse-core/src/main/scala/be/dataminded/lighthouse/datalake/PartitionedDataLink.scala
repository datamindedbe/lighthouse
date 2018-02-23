package be.dataminded.lighthouse.datalake

import org.apache.spark.sql.{DataFrame, Dataset}

class PartitionedDataLink(dataLink: PathBasedDataLink, val partitionPath: LazyConfig[String])
    extends PathBasedDataLink {
  val path: LazyConfig[String] = s"${dataLink.path().trim().stripSuffix("/")}/${partitionPath()}"

  override def doRead(path: String): DataFrame = dataLink.doRead(path)

  override def doWrite[T](data: Dataset[T], path: String): Unit = dataLink.doWrite(data, path)
}
