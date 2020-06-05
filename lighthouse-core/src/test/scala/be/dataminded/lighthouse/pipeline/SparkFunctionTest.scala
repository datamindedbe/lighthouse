package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.Models._
import be.dataminded.lighthouse.testing.{DatasetComparer, SharedSparkSession}
import better.files._
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SparkFunctionTest
    extends AnyFunSpec
    with Matchers
    with SharedSparkSession
    with DatasetComparer
    with BeforeAndAfter {

  import spark.implicits._

  describe("A SparkFunction can be used without SparkSession") {
    it("should be created from a single value") {
      val pipeline = SparkFunction.of(123)

      pipeline.run(spark) should equal(123)
    }

    it("can be mapped with a given function") {
      val pipeline = SparkFunction.of(123).map(number => number * 2)

      pipeline.run(spark) should equal(246)
    }

    it("can be flatMapped with a given function") {
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

  describe("Demonstrate SparkFunctions using a SparkSession") {

    val customerPath: String = Resource.getUrl("customers.csv").getPath()
    val ordersPath: String   = Resource.getUrl("orders.csv").getPath()

    it("A SparkFunction can be used with a SparkSession") {
      val pipeline = SparkFunction { spark =>
        spark.read.option("header", "true").csv(customerPath)
      }.map { customers =>
        customers.count()
      }

      pipeline.run(spark) should equal(3)
    }

    it("Data can be read with the Sources class too") {
      val pipeline = Sources.fromCsv(customerPath).map(customers => customers.count())

      pipeline.run(spark) should equal(3)
    }

    it("For a range of common functions used on Spark Datasets/Dataframes there are shortcuts") {
      val pipeline = Sources.fromCsv(customerPath).count()

      pipeline.run(spark) should equal(3)
    }

    it("A simple business pipeline written using SparkFunctions") {
      val customers = Sources.fromCsv(customerPath)
      val orders    = Sources.fromCsv(ordersPath)

      // I wrap this in a SparkFunction again because I need the SparkSession to import the implicits
      def countOrdersByCustomer(orders: DataFrame): SparkFunction[DataFrame] =
        SparkFunction { spark =>
          import spark.implicits._
          orders.groupBy('customerId).agg(count('id).as("count"))
        }

      // No need for SparkSession, just a normal function
      def joinCustomersWithOrders(customers: DataFrame, ordersByCustomer: DataFrame): DataFrame = {
        customers
          .join(ordersByCustomer, ordersByCustomer("customerId") === customers("id"))
          .select(customers("firstName"), customers("lastName"), ordersByCustomer("count"))
      }

      val ordersByCustomer = orders.flatMap(countOrdersByCustomer)

      val result = (customers, ordersByCustomer).mapN(joinCustomersWithOrders).run(spark)

      val expected = Seq(("Bernard", "Chanson", 5L), ("Ron", "Swanson", 3L), ("Karl", "von Bauchspeck", 2L))
        .toDF("firstName", "lastName", "count")

      assertDatasetEquality(result, expected)
    }

    it("A simple pipeline to test functionality") {
      val persons = SparkFunction { spark: SparkSession =>
        import spark.implicits._
        Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()
      }

      val pipeline = persons
        .map(PersonTransformations.dedup)
        .flatMap(PersonTransformations.normalize)
        .map(PersonTransformations.returnBase)
        .write(OrcSink(File.temporaryFile().get().pathAsString))

      pipeline.run(spark)
    }

    it("A pipeline that combines multiple sources") {
      val persons = SparkFunction { spark: SparkSession =>
        import spark.implicits._
        Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()
      }

      val morePersons = SparkFunction { spark: SparkSession =>
        import spark.implicits._
        Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()
      }

      def combinePersons(left: Dataset[RawPerson], right: Dataset[RawPerson]) = left.union(right)

      val pipeline = for {
        p <- persons
        m <- morePersons
        combinedPersons = combinePersons(p, m)
      } yield PersonTransformations.dedup(combinedPersons)

      assertDatasetEquality(
        pipeline.run(spark),
        Seq(RawPerson("Karl von Bauchspeck", 28), RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35))
          .toDS(),
        orderedComparison = false
      )
    }

    it("A pipeline can write to multiple sinks at once") {
      val persons = SparkFunction { spark: SparkSession =>
        import spark.implicits._
        Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()
      }.map(PersonTransformations.dedup).write(OrcSink("./target/output/orc"), ParquetSink("./target/output/parquet"))

      persons.run(spark)

      val expected =
        Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()

      assertDatasetEquality(spark.read.orc("./target/output/orc").as[RawPerson], expected, orderedComparison = false)
      assertDatasetEquality(
        spark.read.parquet("./target/output/parquet").as[RawPerson],
        expected,
        orderedComparison = false
      )
    }

    object PersonTransformations {

      def dedup(persons: Dataset[RawPerson]): Dataset[RawPerson] = persons.distinct()

      def normalize(persons: Dataset[RawPerson]): SparkFunction[Dataset[BasePerson]] =
        SparkFunction { spark: SparkSession =>
          import spark.implicits._

          persons.map { raw =>
            val tokens = raw.name.split(" ")
            BasePerson(tokens(0), tokens(1), raw.age)
          }
        }

      def returnBase(basePersons: Dataset[BasePerson]): Dataset[BasePerson] = basePersons
    }
  }

  after {
    ("target" / "output").delete(swallowIOExceptions = true)
  }
}
