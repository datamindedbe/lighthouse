package be.dataminded.lighthouse.experimental.datalake.links
import java.time.LocalDate

import be.dataminded.lighthouse.datalake.LazyConfig
import org.apache.spark.sql.Encoder

trait TypedPartitionable[T] extends Partitionable[T, TypedDataLink[T]] {
  self: PathBasedDataStorage with TypedDataLink[T] =>

  override def snapshotOf(date: LazyConfig[LocalDate],
                          pathSuffix: Option[String]): SnapshotDataStorage with TypedDataLink[T] =
    new SnapshotDataStorage(date, pathSuffix) with TypedDataLink[T] with TypedPartitionable[T] {
      override def sourceDataStorage: PathBasedDataStorage = self

      override protected def encoder: Encoder[T] = self.encoder
    }

  override def partitionOf(partitionPath: LazyConfig[String]): PartitionedDataStorage with TypedDataLink[T] =
    new PartitionedDataStorage(partitionPath) with TypedDataLink[T] with TypedPartitionable[T] {
      override def sourceDataStorage: PathBasedDataStorage = self

      override protected def encoder: Encoder[T] = self.encoder
    }
}
