package be.dataminded.lighthouse.experimental.datalake.links
import be.dataminded.lighthouse.datalake.LazyConfig
import org.apache.spark.sql.DataFrame

abstract class PathBasedDataStorage extends DataStorage {
  def path: LazyConfig[String]

  override protected final def doRead: DataFrame              = readFrom(path())
  override protected final def doWrite(data: DataFrame): Unit = writeTo(path(), data)

  protected[links] def readFrom(path: String): DataFrame
  protected[links] def writeTo(path: String, data: DataFrame): Unit

}
