package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.datalake.DataLink
import org.apache.spark.sql.DataFrame

object Sources {

  def fromDataLink(dataLink: DataLink): SparkFunction[DataFrame] = SparkFunction.of(dataLink.read())

  def fromOrc(path: String): SparkFunction[DataFrame] =
    SparkFunction { spark =>
      spark.read.orc(path)
    }

  def fromText(path: String) =
    SparkFunction { spark =>
      import spark.implicits._
      spark.read.text(path).as[String]
    }

  def fromCsv(path: String): SparkFunction[DataFrame] =
    SparkFunction { spark =>
      spark.read.option("inferSchema", "true").option("header", "true").csv(path)
    }
}
