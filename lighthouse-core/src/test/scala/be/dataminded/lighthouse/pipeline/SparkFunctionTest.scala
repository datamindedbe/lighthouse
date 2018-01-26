package be.dataminded.lighthouse.pipeline

import cats.implicits._
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

class SparkFunctionTest extends FunSpec with Matchers {
  describe("A SparkFunction") {
    val spark: SparkSession = null

    it("should be created from a single value") {
      val pipeline = SparkFunction.of(123)

      pipeline.run(spark) should equal(123)
    }

    it("can be mapped with a given function") {
      val pipeline = SparkFunction.of(123).map(number => number * 2)

      pipeline.run(spark) should equal(246)
    }

    it("cab be flatMapped with a given function") {
      val pipeline = SparkFunction.of(123).flatMap(number => SparkFunction.of(number * 2))

      pipeline.run(spark) should equal(246)
    }

    it("two SparkFunctions can be joined together") {
      val result = for {
        first  <- SparkFunction.of("ab")
        second <- SparkFunction.of("cd")
      } yield first + second

      result.run(spark) should equal("abcd")
    }

    it("two SparkFunctions can be joined together using an ordinary function") {
      def mergeTwoWords(first: String, second: String) = first + second
      val result = for {
        first  <- SparkFunction.of("ab")
        second <- SparkFunction.of("cd")
      } yield mergeTwoWords(first, second)

      result.run(spark) should equal("abcd")
    }

    it("the number of things that can be used together is not limited") {
      def lotsOfTypes(int: Int, string: String, boolean: Boolean, double: Double) =
        Seq(int, string, boolean, double).mkString(" ")

      val result = for {
        int     <- SparkFunction.of(1)
        string  <- SparkFunction.of("A")
        boolean <- SparkFunction.of(true)
        double  <- SparkFunction.of(123.0)
      } yield lotsOfTypes(int, string, boolean, double)

      result.run(spark) should equal("1 A true 123.0")
    }

    it("two SparkFunctions can be joined together using some magic from the Cats-library") {

      def mergeTwoWords(first: String, second: String) = first + second

      val first  = SparkFunction.of("ab")
      val second = SparkFunction.of("cd")

      val result = (first, second).mapN(mergeTwoWords)

      result.run(spark) should equal("abcd")
    }

    it("can be used to build more complex pipelines") {
      val a = SparkFunction.of("a")
      val b = SparkFunction.of("b")
      val c = SparkFunction.of("c")
      val d = SparkFunction.of("d")
      val e = SparkFunction.of("e")

      def genericJoinFunction(left: String, right: String) = left + right

      val pipeline = for {
        a <- a
        b <- b
        aAndB = genericJoinFunction(a, b)
        c <- c
        abAndC = genericJoinFunction(aAndB, c)
        d <- d
        e <- e
        dAndE = genericJoinFunction(d, e)
      } yield genericJoinFunction(abAndC, dAndE)

      pipeline.run(spark) should equal("abcde")
    }
  }
}
