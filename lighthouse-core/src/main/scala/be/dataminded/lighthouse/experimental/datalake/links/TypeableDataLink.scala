package be.dataminded.lighthouse.experimental.datalake.links
import org.apache.spark.sql.Dataset

trait TypeableDataLink[T] extends Serializable {
    this: DataStorage =>

    def read: Dataset[T]
    def write(data: Dataset[T]): Unit
  }
