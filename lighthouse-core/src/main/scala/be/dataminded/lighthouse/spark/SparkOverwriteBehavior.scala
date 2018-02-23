package be.dataminded.lighthouse.spark

/**
  * Type-safe objects to express partition overwrite behavior
  */
sealed trait SparkOverwriteBehavior {
  val name: String
  override def toString: String = name
}

case object FullOverwrite extends SparkOverwriteBehavior { val name = "FullOverwrite" }

case object PartitionOverwrite extends SparkOverwriteBehavior { val name = "PartitionOverwrite" }
