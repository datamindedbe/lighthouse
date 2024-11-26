package be.dataminded.lighthouse.experimental.datalake.links

import java.time.LocalDate

import be.dataminded.lighthouse.datalake.LazyConfig

trait Partitionable[T, L <: TypeableDataLink[T]] {
  this: PathBasedDataStorage with TypeableDataLink[T] =>

  def snapshotOf(date: LazyConfig[LocalDate],
                 pathSuffix: Option[String] = None): SnapshotDataStorage with L


  def partitionOf(partitionPath: LazyConfig[String]): PartitionedDataStorage with L

}
