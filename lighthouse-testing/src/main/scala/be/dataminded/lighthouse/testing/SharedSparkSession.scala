package be.dataminded.lighthouse.testing

import better.files.File
import org.apache.spark.sql.SparkSession

trait SharedSparkSession {

  private object AutoImport {
    lazy val tmp: File = File.newTemporaryDirectory().deleteOnExit()

    lazy val localMetastorePath: String  = (tmp / "metastore").pathAsString
    lazy val localWarehousePath: String  = (tmp / "warehouse").pathAsString
    lazy val checkPointDirectory: String = (tmp / "checkpoint").pathAsString
  }

  import AutoImport._

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("LocalSparkTesting")
    .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
    .config("datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
    .config("spark.sql.streaming.checkpointLocation", checkPointDirectory)
    .config("spark.sql.warehouse.dir", localWarehousePath)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.avro.compression.codec", "snappy")
    .config("spark.sql.orc.compression.codec", "snappy")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .enableHiveSupport()
    .getOrCreate()
}
