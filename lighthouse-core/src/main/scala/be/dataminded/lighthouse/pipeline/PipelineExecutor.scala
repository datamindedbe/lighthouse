package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.spark.SparkSessions
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Try}

/**
  * Un-typed pipeline executor, enables creation of mock executors like the [[DummyExecutor]]
  */
sealed trait PipelineExecutorU {
  def execute[A](function: SparkFunction[A]): Any
}

/**
  * Executor that doesn't provide a [[org.apache.spark.sql.SparkSession]], mainly used to write faster tests
  */
sealed trait DummyExecutor extends PipelineExecutorU {
  override def execute[A](function: SparkFunction[A]): A = function.run(null)
}

sealed trait PipelineExecutor[B] extends PipelineExecutorU {
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
