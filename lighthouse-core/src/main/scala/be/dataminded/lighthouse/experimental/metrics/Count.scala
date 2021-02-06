package be.dataminded.lighthouse.experimental.metrics
import org.apache.spark.sql.Row

class Count[N: Numeric] extends Metric[N] {
  private val numeric: Numeric[N] = implicitly

  override def ofRow(row: Row): N     = numeric.one
  override def combine(x: N, y: N): N = numeric.plus(x, y)
}

object Count {
  implicit def count[N: Numeric]: Count[N] = new Count[N]
}