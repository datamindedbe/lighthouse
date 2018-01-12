package be.dataminded.lighthouse.datalake

import be.dataminded.lighthouse.spark.{Orc, SparkFileFormat}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
  * Reference to data stored on hive
  *
  * @constructor create a new hive data reference
  * @param path      the location on the file system
  * @param format        the format the data is stored in
  * @param saveMode      the save mode to apply (overwrite, append)
  * @param partitionedBy the partitioning to apply
  * @param database      the name of the database
  * @param table         the name of the table
  */
class HiveDataLink(val path: LazyConfig[String],
                   database: LazyConfig[String],
                   table: LazyConfig[String],
                   format: SparkFileFormat = Orc,
                   saveMode: SaveMode = SaveMode.Overwrite,
                   partitionedBy: List[String] = List.empty)
    extends PathBasedDataLinkTemplate {

  override def doRead(path: String): DataFrame = {
    spark.catalog.setCurrentDatabase(database())
    spark.read.table(table())
  }

  override def doWrite[T](dataset: Dataset[T], path: String): Unit = {
    spark.catalog.setCurrentDatabase(database())
    dataset.write
      .format(format.toString)
      .partitionBy(partitionedBy: _*)
      .mode(saveMode)
      .option("path", path)
      .saveAsTable(table())
  }
}
