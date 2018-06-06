package be.dataminded.lighthouse.datalake

import be.dataminded.lighthouse.spark.{Orc, SparkFileFormat}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
  * Reference to data stored on the file system
  *
  * @constructor create a new file system data reference
  * @param path          the location on the file system
  * @param format        the format the data is stored in
  * @param saveMode      the save mode to apply (overwrite, append)
  * @param partitionedBy the partitioning to apply
  */
class FileSystemDataLink(val path: LazyConfig[String],
                         format: SparkFileFormat = Orc,
                         saveMode: SaveMode = SaveMode.Overwrite,
                         partitionedBy: List[String] = List.empty,
                         options: Map[String, String] = Map.empty,
                         schema: Option[StructType] = None)
    extends PathBasedDataLink {

  override def doRead(path: String): DataFrame = {
    schema match {
      case Some(s) => spark.read.format(format.toString).options(options).schema(s).load(path)
      case None    => spark.read.format(format.toString).options(options).load(path)
    }
  }

  override def doWrite[T](dataset: Dataset[T], path: String): Unit = {
    dataset.write
      .format(format.toString)
      .partitionBy(partitionedBy: _*)
      .options(options)
      .mode(saveMode)
      .save(path)
  }
}
