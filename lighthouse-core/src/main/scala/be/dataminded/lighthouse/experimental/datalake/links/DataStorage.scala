package be.dataminded.lighthouse.experimental.datalake.links
import org.apache.spark.sql.DataFrame

trait DataStorage {
  protected def doRead: DataFrame

  protected def doWrite(data: DataFrame): Unit
}
