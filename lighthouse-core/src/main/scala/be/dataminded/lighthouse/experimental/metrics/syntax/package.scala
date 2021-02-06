package be.dataminded.lighthouse.experimental.metrics
import org.apache.spark.sql.Row

package object syntax {
  trait MetricTag[T, M <: Metric[T]]
  type As[T, M <: Metric[T]] = T with MetricTag[T, M]

  implicit class MetricsSyntax[T, M <: Metric[T]](x: T As M)(implicit metric: M) {
    def ++(y: T As M): T As M =
      implicitly[M].combine(x, y).asInstanceOf[T As M]
  }

  implicit class RowSyntax(row: Row) {
    def calculateThisMetric[T, M <: Metric[T]](implicit metric: M): T As M =
      implicitly[M].ofRow(row).asInstanceOf[T As M]
  }
}
