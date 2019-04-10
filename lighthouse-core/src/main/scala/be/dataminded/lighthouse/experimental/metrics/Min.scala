package be.dataminded.lighthouse.experimental.metrics

import org.apache.spark.sql.Row

class Min[O: Ordering](field: String) extends Metric[O] {
  private val ordering: Ordering[O] = implicitly

  override def ofRow(row: Row): O = row.getAs(field)

  override def combine(x: O, y: O): O = ordering.min(x, y)
}