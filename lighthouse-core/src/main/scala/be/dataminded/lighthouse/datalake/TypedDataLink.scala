package be.dataminded.lighthouse.datalake
import org.apache.spark.sql.{Dataset, Encoder}

class TypedDataLink[T: Encoder](link: DataLink) {
  def readTyped(): Dataset[T] = link.readAs[T]()

  def writeTyped(ds: Dataset[T]): Unit = link.write[T](ds)

}
