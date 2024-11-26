package be.dataminded.lighthouse.experimental.metrics

import org.apache.spark.sql.Row

class NullCount[N: Numeric](field: String) extends Metric[N] {
  private val numeric: Numeric[N] = implicitly

  override def ofRow(row: Row): N =
    if (row.isNullAt(row.fieldIndex(field)))
      numeric.one
    else
      numeric.zero

  override def combine(x: N, y: N): N = numeric.plus(x, y)
}