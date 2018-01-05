package be.dataminded.lighthouse.pipeline

import org.apache.spark.sql.DataFrame

object Sources {

  def fromOrc(path: String): SparkFunction[DataFrame] = SparkFunction { spark =>
    spark.read.orc(path)
  }

  def fromText(path: String) = SparkFunction { spark =>
    import spark.implicits._
    spark.read.text(path).as[String]
  }

  def fromCsv(path: String): SparkFunction[DataFrame] = SparkFunction { spark =>
    spark.read.option("inferSchema", "true").option("header", "true").csv(path)
  }
}
