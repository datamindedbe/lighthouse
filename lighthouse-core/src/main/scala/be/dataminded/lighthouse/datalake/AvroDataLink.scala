package be.dataminded.lighthouse.datalake

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

class AvroDataLink(
    val path: LazyConfig[String],
    saveMode: SaveMode = SaveMode.Overwrite,
    partitionedBy: List[String] = List.empty,
    options: Map[String, String] = Map.empty
) extends PathBasedDataLink {

  override def doRead(path: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.avro")
      .options(options)
      .load(path)
  }

  override def doWrite[T](dataset: Dataset[T], path: String): Unit = {
    dataset.write
      .format("com.databricks.spark.avro")
      .partitionBy(partitionedBy: _*)
      .options(options)
      .mode(saveMode)
      .save(path)
  }
}
