package be.dataminded.lighthouse.spark

/**
  * Type-safe objects to express Spark SQL file-formats
  */
sealed trait SparkFileFormat {
  val name: String
  override def toString: String = name
}

case object Orc extends SparkFileFormat { val name = "orc" }

case object Parquet extends SparkFileFormat { val name = "parquet" }

case object Csv extends SparkFileFormat { val name = "csv" }

case object Json extends SparkFileFormat { val name = "json" }

case object Text extends SparkFileFormat { val name = "text" }
