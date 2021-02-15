package be.dataminded.lighthouse.datalake

import be.dataminded.lighthouse.spark.SparkOverwriteBehavior._
import be.dataminded.lighthouse.spark._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
  * Reference to data stored on hive
  *
  * For different combinations with saveMode and overwriteBehavior different implementations are used.
  * The cases are:
  *
  * (SaveMode.Overwrite, PartitionOverwrite) => In this case we can overwrite one partition for example the ingest date.
  * You can find an example in the HiveDataLinkTest.
  *
  * (SaveMode.Overwrite, MultiplePartitionOverwrite) => This case we overwrite multiple partitions at the same time.
  *
  * _ => All the other cases.
  *
  * You can see the code on how they are implemented.
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
class HiveDataLink(
    val path: LazyConfig[String],
    database: LazyConfig[String],
    table: LazyConfig[String],
    format: SparkFileFormat = Orc,
    saveMode: SaveMode = SaveMode.Overwrite,
    partitionedBy: List[String] = Nil,
    overwriteBehavior: SparkOverwriteBehavior = FullOverwrite,
    options: Map[String, String] = Map.empty
) extends PathBasedDataLink {

  override def doRead(path: String): DataFrame = {
    spark.catalog.setCurrentDatabase(database())
    spark.read.table(table())
  }

  override def doWrite[T](dataset: Dataset[T], path: String): Unit = {
    spark.catalog.setCurrentDatabase(database())

    val baseWriter = dataset.write
      .format(format.toString)
      .mode(saveMode)
      .options(options)

    (saveMode, overwriteBehavior) match {
      // Only overwrite a single partition in this case
      case (SaveMode.Overwrite, PartitionOverwrite) =>
        // It works fine with Spark below 2.3.0
        // Extract the base path based on the full path and partitionedBy information. Ordering matters!
        val basePath = partitionedBy.reverse.foldLeft(path.trim().stripSuffix("/")) {
          case (p, ptn) => p.substring(0, p.lastIndexOf(ptn)).trim().stripSuffix("/")
        }

        // If table does not exist yet, create new table
        if (!spark.catalog.tableExists(table())) {
          // Create table
          baseWriter
            .partitionBy(partitionedBy: _*)
            .option("path", basePath)
            .saveAsTable(table())
        } else {
          // Write the dataset to the desired location. Don't use partitionBy as it lines up with path
          baseWriter.save(path)

          // Update partition information based on
          spark.catalog.recoverPartitions(table())
        }

      case (SaveMode.Overwrite, MultiplePartitionOverwrite) =>
        // Valid since spark 2.3.0 only
        // Make sure that conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        // If table does not exist yet, create new table
        val partitionOverwriteMode = spark.conf.get("spark.sql.sources.partitionOverwriteMode")
        assert(
          partitionOverwriteMode == "dynamic",
          s"""MultiplePartitionOverwrite can only be used with partitionOverwriteMode='dynamic' set and Spark since 2.3.0.
            |Current Spark version: ${spark.version}
            |Current partitionOverwriteMode value: $partitionOverwriteMode
          """.stripMargin
        )

        if (!spark.catalog.tableExists(table())) {
          // Create table
          baseWriter
            .partitionBy(partitionedBy: _*)
            .option("path", path)
            .saveAsTable(table())
        } else {
          baseWriter.insertInto(table())
        }

      // In any other case use default spark write behavior
      case _ =>
        // Just use the default spark/hive write behavior
        baseWriter
          .partitionBy(partitionedBy: _*)
          .option("path", path)
          .saveAsTable(table())
    }
  }
}
