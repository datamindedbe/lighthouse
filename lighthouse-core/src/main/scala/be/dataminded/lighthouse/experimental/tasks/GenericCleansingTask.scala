package be.dataminded.lighthouse.experimental.tasks
import be.dataminded.lighthouse.experimental.datalake.links.{TypedDataLink, UntypedDataLink}
import be.dataminded.lighthouse.experimental.metrics.MetricsBundle
import org.apache.spark.sql.{Dataset, Encoder, Row}

abstract class GenericCleansingTask[O: Encoder, B: MetricsBundle] extends Serializable {
  private val metricsBundle: MetricsBundle[B] = implicitly

  private val sourceData = source.read.cache()

  def source: UntypedDataLink
  def sink: TypedDataLink[O]

  def trasform(row: Row): O

  def clean: Dataset[O] = sourceData.map(trasform)

  def metrics: B = {
    import metricsBundle.encoder
    sourceData.map(metricsBundle.ofRow).reduce(metricsBundle.combine _)
  }
}
