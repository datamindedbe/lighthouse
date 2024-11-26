package be.dataminded.lighthouse.experimental.datalake.links

import org.apache.spark.sql.DataFrame

trait ProxyPathBasedDataStorage extends PathBasedDataStorage {
  def sourceDataStorage: PathBasedDataStorage

  override protected[links] def readFrom(path: String): DataFrame            = sourceDataStorage.readFrom(path)
  override protected[links] def writeTo(path: String, data: DataFrame): Unit = sourceDataStorage.writeTo(path, data)
}
