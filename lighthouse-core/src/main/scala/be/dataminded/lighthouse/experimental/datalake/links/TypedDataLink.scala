package be.dataminded.lighthouse.experimental.datalake.links
import org.apache.spark.sql.{Dataset, Encoder}

trait TypedDataLink[T] extends TypeableDataLink[T] {
  this: DataStorage =>

  protected def encoder: Encoder[T]

  override def read: Dataset[T]              = doRead.as[T](encoder)
  override def write(data: Dataset[T]): Unit = doWrite(data.toDF())
}
