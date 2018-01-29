package be.dataminded.lighthouse.pipeline

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SaveMode}

import scala.util.{Failure, Success, Try}

sealed trait SaveStatus

case object SaveSuccess extends SaveStatus

case object SaveFailure extends SaveStatus

trait Sink {
  def write(data: Dataset[_]): SaveStatus
}

class OrcSink(path: String, mode: SaveMode = SaveMode.Overwrite) extends Sink with LazyLogging {
  def write(dataset: Dataset[_]): SaveStatus = {
    Try(dataset.write.mode(mode).option("compression", "zlib").orc(path)) match {
      case Success(_) => SaveSuccess
      case Failure(e) =>
        logger.warn("Something went wrong writing the dataset", e)
        SaveFailure
    }
  }
}

object OrcSink {
  def apply(path: String): OrcSink = new OrcSink(path)
}

class PartitionedOrcSink(path: String, partitionsColumns: Seq[String], mode: SaveMode) extends Sink with LazyLogging {
  def write(dataset: Dataset[_]): SaveStatus = {
    Try(
      dataset.write
        .mode(mode)
        .option("compression", "zlib")
        .partitionBy(partitionsColumns: _*)
        .orc(path)) match {
      case Success(_) => SaveSuccess
      case Failure(e) =>
        logger.warn("Something went wrong writing the dataset", e)
        SaveFailure
    }
  }
}

object PartitionedOrcSink {
  def apply(path: String, partitionsColumns: Seq[String], mode: SaveMode = SaveMode.Append): PartitionedOrcSink =
    new PartitionedOrcSink(path, partitionsColumns, mode)
}

class TextSink(path: String, mode: SaveMode = SaveMode.Overwrite) extends Sink with LazyLogging {
  def write(dataset: Dataset[_]): SaveStatus = {
    Try(dataset.write.mode(mode).text(path)) match {
      case Success(_) => SaveSuccess
      case Failure(e) =>
        logger.warn("Something went wrong writing the dataset", e)
        SaveFailure
    }
  }
}

object TextSink {
  def apply(path: String, mode: SaveMode = SaveMode.Overwrite): TextSink =
    new TextSink(path, mode)
}

class ParquetSink(path: String) extends Sink with LazyLogging {
  def write(dataset: Dataset[_]): SaveStatus = {
    Try(dataset.write.parquet(path)) match {
      case Success(_) => SaveSuccess
      case Failure(e) =>
        logger.warn("Something went wrong writing the dataset", e)
        SaveFailure
    }
  }
}

object ParquetSink {
  def apply(path: String): ParquetSink = new ParquetSink(path)
}

class SingleFileSink(sink: Sink) extends Sink {
  override def write(data: Dataset[_]): SaveStatus = {
    sink.write(data.repartition(1))
  }
}

object SingleFileSink {
  def apply(sink: Sink): SingleFileSink = new SingleFileSink(sink)
}
