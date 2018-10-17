package be.dataminded.lighthouse.datalake

import be.dataminded.lighthouse.spark._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
  * Reference to data stored on hive
  *
  * @constructor create a new hive data reference
  * @param path               The location on the file system
  * @param format             The format the data is stored in
  * @param saveMode           The save mode to apply (overwrite, append)
  * @param partitionedBy      The partitioning to apply, these partition strings should be columns in the provided table.
  *                           Ordering matters!
  * @param overwriteBehavior  Defines the behavior when using partitions, overwrite everything, or only a single
  *                           partition
  * @param database           The name of the database
  * @param table              The name of the table
  */
class HiveDataLink(val path: LazyConfig[String],
                   database: LazyConfig[String],
                   table: LazyConfig[String],
                   format: SparkFileFormat = Orc,
                   saveMode: SaveMode = SaveMode.Overwrite,
                   partitionedBy: List[String] = Nil,
                   overwriteBehavior: SparkOverwriteBehavior = FullOverwrite,
                   options: Map[String, String] = Map.empty)
    extends PathBasedDataLink {

  override def doRead(path: String): DataFrame = {
    spark.catalog.setCurrentDatabase(database())
    spark.read.table(table())
  }

  override def doWrite[T](dataset: Dataset[T], path: String): Unit = {
    spark.catalog.setCurrentDatabase(database())

    (saveMode, overwriteBehavior) match {
      // Only overwrite a single partition in this case
      case (SaveMode.Overwrite, PartitionOverwrite) =>
        // Extract the base path based on the full path and partitionedBy information. Ordering matters!
        val basePath = partitionedBy.reverse.foldLeft(path.trim().stripSuffix("/")) {
          case (p, ptn) => p.substring(0, p.lastIndexOf(ptn)).trim().stripSuffix("/")
        }

        // If table does not exist yet, create new table
        if (!spark.catalog.tableExists(table())) {
          // Create table
          dataset.write
            .format(format.toString)
            .partitionBy(partitionedBy: _*)
            .mode(saveMode)
            .option("path", basePath)
            .options(options)
            .saveAsTable(table())
        } else {
          // Write the dataset to the desired location. Don't use partitionBy as it lines up with path
          dataset.write
            .format(format.toString)
            .mode(saveMode)
            .options(options)
            .save(path)

          // Update partition information based on
          spark
            .sql(s"ALTER TABLE ${table()} RECOVER PARTITIONS")
            .foreach(_ => ()) // execute DataFrame
        }

      // In any other case use default spark write behavior
      case _ =>
        // Just use the default spark/hive write behavior
        dataset.write
          .format(format.toString)
          .partitionBy(partitionedBy: _*)
          .mode(saveMode)
          .option("path", path)
          .options(options)
          .saveAsTable(table())
    }
  }
}
