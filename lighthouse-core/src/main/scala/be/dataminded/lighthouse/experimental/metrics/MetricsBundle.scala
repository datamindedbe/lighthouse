package be.dataminded.lighthouse.experimental.metrics
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.ClassTag

abstract class MetricsBundle[B: ClassTag] extends Metric[B] with Serializable {
  implicit val encoder: Encoder[B] = Encoders.kryo
//  def ofRow(row: Row): B
//  def combine(x: B, y: B)
}

/*
object MetricsBundle {
  implicit def bundle[B]: MetricsBundle[B]
}*/
