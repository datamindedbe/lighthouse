package be.dataminded.lighthouse.experimental.metrics
import org.apache.spark.sql.Row

trait Metric[M] {
  def ofRow(row: Row): M

  def combine(x: M, y: M): M
}
