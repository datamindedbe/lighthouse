package be.dataminded.lighthouse.experimental.datalake.links
import be.dataminded.lighthouse.spark.SparkSessions
import org.apache.spark.sql.{DataFrame, Encoder}

class MockDataStorage(data: DataFrame) extends DataStorage {
  override protected[links] def doRead: DataFrame              = data
  override protected[links] def doWrite(data: DataFrame): Unit = ()
}

object MockDataStorage extends SparkSessions {
  def typed[T: Encoder](data: DataFrame): TypedDataLink[T] = new MockDataStorage(data) with TypedDataLink[T] {
    override protected def encoder: Encoder[T] = implicitly
  }

  def untyped(data: DataFrame): UntypedDataLink = new MockDataStorage(data) with UntypedDataLink {}
}
