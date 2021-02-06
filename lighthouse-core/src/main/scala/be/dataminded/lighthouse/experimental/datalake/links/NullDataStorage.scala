package be.dataminded.lighthouse.experimental.datalake.links

import be.dataminded.lighthouse.spark.SparkSessions
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

class NullDataStorage private(sparkSession: SparkSession) extends DataStorage {
  override protected[links] def doRead: DataFrame              = sparkSession.createDataFrame(Seq.empty)
  override protected[links] def doWrite(data: DataFrame): Unit = ()
}

object NullDataStorage extends SparkSessions {
  def typed[T: Encoder]: TypedDataLink[T] = new NullDataStorage(spark) with TypedDataLink[T] {
    override protected def encoder: Encoder[T] = implicitly
  }

  def untyped = new NullDataStorage(spark) with UntypedDataLink
}
