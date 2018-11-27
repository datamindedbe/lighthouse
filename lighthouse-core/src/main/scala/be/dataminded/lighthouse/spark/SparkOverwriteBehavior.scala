package be.dataminded.lighthouse.spark

/**
  * Type-safe objects to express partition overwrite behavior
  */
sealed abstract class SparkOverwriteBehavior(name: String) {
  override def toString: String = name
}

object SparkOverwriteBehavior {

  case object FullOverwrite extends SparkOverwriteBehavior("FullOverwrite")

  case object PartitionOverwrite extends SparkOverwriteBehavior("PartitionOverwrite")

  case object MultiplePartitionOverwrite extends SparkOverwriteBehavior("MultiplePartitionOverwrite")

}
