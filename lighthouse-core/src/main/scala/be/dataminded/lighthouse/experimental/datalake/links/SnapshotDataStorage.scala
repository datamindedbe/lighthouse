package be.dataminded.lighthouse.experimental.datalake.links

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import be.dataminded.lighthouse.datalake._

abstract class SnapshotDataStorage(val snapshotDate: LazyConfig[LocalDate], val pathSuffix: Option[String])
    extends ProxyPathBasedDataStorage {

  override val path: LazyConfig[String] =
    s"${sourceDataStorage.path().trim().stripSuffix("/")}/${snapshotDate().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))}/${pathSuffix
      .getOrElse("")}"
}

object SnapshotDataStorage {
  def apply[T](dataLink: PathBasedDataStorage,
               snapshotDate: LazyConfig[LocalDate],
               pathSuffix: Option[String]): SnapshotDataStorage = {

    new SnapshotDataStorage /*[T]*/ (snapshotDate, pathSuffix) {
      override val sourceDataStorage: PathBasedDataStorage = dataLink
    }
  }
}
