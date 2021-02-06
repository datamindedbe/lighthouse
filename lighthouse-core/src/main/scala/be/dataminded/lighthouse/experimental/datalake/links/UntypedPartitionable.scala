package be.dataminded.lighthouse.experimental.datalake.links

import java.time.LocalDate

import be.dataminded.lighthouse.datalake.LazyConfig
import org.apache.spark.sql.Row

trait UntypedPartitionable extends Partitionable[Row, UntypedDataLink] {
  self: PathBasedDataStorage with UntypedDataLink =>

  override def snapshotOf(date: LazyConfig[LocalDate],
                          pathSuffix: Option[String]): SnapshotDataStorage with UntypedDataLink =
    new SnapshotDataStorage(date, pathSuffix) with UntypedDataLink with UntypedPartitionable {
      override def sourceDataStorage: PathBasedDataStorage = self
    }

  override def partitionOf(partitionPath: LazyConfig[String]): PartitionedDataStorage with UntypedDataLink =
    new PartitionedDataStorage(partitionPath) with UntypedDataLink with UntypedPartitionable {
      override def sourceDataStorage: PathBasedDataStorage = self
    }
}
