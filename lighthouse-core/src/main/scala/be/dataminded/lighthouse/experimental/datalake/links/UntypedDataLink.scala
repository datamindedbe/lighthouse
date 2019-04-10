package be.dataminded.lighthouse.experimental.datalake.links
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait UntypedDataLink extends TypeableDataLink[Row] {
  this: DataStorage =>

  override def read: Dataset[Row]           = doRead
  override def write(data: DataFrame): Unit = doWrite(data)
}
