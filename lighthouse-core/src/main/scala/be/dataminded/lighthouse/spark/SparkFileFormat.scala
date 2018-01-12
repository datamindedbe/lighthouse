package be.dataminded.lighthouse.spark

/**
  * Type-safe objects to express Spark SQL file-formats
  */
sealed trait SparkFileFormat

case object Orc extends SparkFileFormat {
  override def toString: String = "orc"
}

case object Parquet extends SparkFileFormat {
  override def toString: String = "parquet"
}

case object Csv extends SparkFileFormat {
  override def toString: String = "csv"
}

case object Json extends SparkFileFormat {
  override def toString: String = "json"
}
