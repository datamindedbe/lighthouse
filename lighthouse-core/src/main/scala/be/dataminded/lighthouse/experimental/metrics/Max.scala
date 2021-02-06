package be.dataminded.lighthouse.experimental.metrics

import org.apache.spark.sql.Row

class Max[O: Ordering](field: String) extends Metric[O] {
  private val ordering: Ordering[O] = implicitly

  override def ofRow(row: Row): O = row.getAs[O](field)

  override def combine(x: O, y: O): O = ordering.max(x, y)
}