package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.spark.SparkSessions
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Try}

sealed trait PipelineExecutor[B] {
  def execute[A](function: SparkFunction[A]): B
}

object PipelineExecutor extends PipelineExecutor[Try[_]] with LazyLogging with SparkSessions {

  override def execute[A](function: SparkFunction[A]): Try[A] = {
    Try {
      logger.info("Executing function: {}", function.getClass.getName)
      function.run(spark)
    }.recoverWith {
      case e: Exception =>
        logger.error("An error occurred when executing the pipeline", e)
        spark.close()
        Failure(e)
    }
  }
}
