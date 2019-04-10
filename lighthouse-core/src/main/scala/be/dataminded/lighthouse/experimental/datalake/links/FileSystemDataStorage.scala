package be.dataminded.lighthouse.experimental.datalake.links

import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.spark.{Orc, SparkFileFormat, SparkSessions}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SaveMode}
import org.apache.spark.sql.types.StructType

abstract class FileSystemDataStorage(val path: LazyConfig[String],
                                     format: SparkFileFormat = Orc,
                                     saveMode: SaveMode = SaveMode.Overwrite,
                                     partitionedBy: List[String] = List.empty,
                                     options: Map[String, String] = Map.empty,
                                     schema: Option[StructType] = None)
    extends PathBasedDataStorage
    with SparkSessions {

  override protected[links] def readFrom(path: String): DataFrame = schema match {
    case Some(s) => spark.read.format(format.toString).options(options).schema(s).load(path)
    case None    => spark.read.format(format.toString).options(options).load(path)
  }

  override protected[links] def writeTo(path: String, data: DataFrame): Unit =
    data.write
      .format(format.toString)
      .partitionBy(partitionedBy: _*)
      .options(options)
      .mode(saveMode)
      .save(path)
}

object FileSystemDataStorage {
  def untyped(path: LazyConfig[String],
              format: SparkFileFormat = Orc,
              saveMode: SaveMode = SaveMode.Overwrite,
              partitionedBy: List[String] = List.empty,
              options: Map[String, String] = Map.empty,
              schema: Option[StructType] = None): FileSystemDataStorage with UntypedDataLink with UntypedPartitionable =
    new FileSystemDataStorage(path, format, saveMode, partitionedBy, options, schema) with UntypedDataLink
    with UntypedPartitionable

  def typed[T: Encoder](
      path: LazyConfig[String],
      format: SparkFileFormat = Orc,
      saveMode: SaveMode = SaveMode.Overwrite,
      partitionedBy: List[String] = List.empty,
      options: Map[String, String] = Map.empty,
      schema: Option[StructType] = None): FileSystemDataStorage with TypedDataLink[T] with TypedPartitionable[T] =
    new FileSystemDataStorage(path, format, saveMode, partitionedBy, options, schema) with TypedDataLink[T]
    with TypedPartitionable[T] {
      override protected def encoder: Encoder[T] = implicitly
    }
}
