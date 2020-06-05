package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.pipeline.SparkFunction.monad
import cats.Monad
import org.apache.spark.sql.SparkSession

trait SparkFunction[+A] {

  def run(spark: SparkSession): A

  // Enables the box, map, flatMap and for-comprehension support
  def map[B](f: A => B): SparkFunction[B] = monad.map(this)(f)

  def flatMap[B](f: A => SparkFunction[B]): SparkFunction[B] =
    monad.flatMap(this)(f)
}

object SparkFunction {

  def apply[A](function: SparkSession => A): SparkFunction[A] =
    new SparkFunction[A] {
      override def run(spark: SparkSession): A = function(spark)
    }

  def of[A](block: => A): SparkFunction[A] =
    new SparkFunction[A] {
      override def run(spark: SparkSession): A = block
    }

  implicit val monad: Monad[SparkFunction] = new Monad[SparkFunction] {
    override def pure[A](x: A): SparkFunction[A] = SparkFunction(_ => x)

    override def flatMap[A, B](fa: SparkFunction[A])(f: A => SparkFunction[B]): SparkFunction[B] =
      SparkFunction(spark => f(fa.run(spark)).run(spark))

    override def tailRecM[A, B](a: A)(f: A => SparkFunction[Either[A, B]]): SparkFunction[B] =
      flatMap(f(a)) {
        case Right(b)    => pure(b)
        case Left(nextA) => tailRecM(nextA)(f)
      }
  }
}
