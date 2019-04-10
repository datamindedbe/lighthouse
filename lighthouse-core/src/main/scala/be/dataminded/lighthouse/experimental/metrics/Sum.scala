package be.dataminded.lighthouse.experimental.metrics

import org.apache.spark.sql.Row

class Sum[N: Numeric](field: String) extends Metric[N] {
  private val numeric: Numeric[N] = implicitly

  override def ofRow(row: Row): N = row.getAs[N](field)

  override def combine(x: N, y: N): N = numeric.plus(x, y)
}