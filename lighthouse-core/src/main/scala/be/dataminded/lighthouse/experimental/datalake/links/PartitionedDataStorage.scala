package be.dataminded.lighthouse.experimental.datalake.links

import be.dataminded.lighthouse.datalake._

abstract class PartitionedDataStorage(val partitionPath: LazyConfig[String]) extends ProxyPathBasedDataStorage {
  override val path: LazyConfig[String] = s"${sourceDataStorage.path().trim().stripSuffix("/")}/${partitionPath()}"
}
