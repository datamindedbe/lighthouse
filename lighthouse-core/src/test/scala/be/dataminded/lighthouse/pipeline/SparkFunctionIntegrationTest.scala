package be.dataminded.lighthouse.pipeline

import be.dataminded.lighthouse.testing.{DatasetComparer, SparkFunSuite}
import better.files._
import cats.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, Matchers}

case class RawPerson(name: String, age: Int)
case class BasePerson(firstName: String, lastName: String, age: Int)

class SparkFunctionIntegrationTest extends SparkFunSuite with Matchers with BeforeAndAfter with DatasetComparer {

  import spark.implicits._

  val customerPath: String = File.resource("customers.csv").pathAsString
  val ordersPath: String   = File.resource("orders.csv").pathAsString

  sparkTest("A SparkFunction can be used with a SparkSession") {
    val pipeline = SparkFunction { spark =>
      spark.read.option("header", "true").csv(customerPath)
    }.map { customers =>
      customers.count()
    }

    pipeline.run(spark) should equal(3)
  }

  sparkTest("Data can be read with the Sources class too") {
    val pipeline = Sources.fromCsv(customerPath).map(customers => customers.count())

    pipeline.run(spark) should equal(3)
  }

  sparkTest("For a range of common functions used on Spark Datasets/Dataframes there are shortcuts") {
    val pipeline = Sources.fromCsv(customerPath).count()

    pipeline.run(spark) should equal(3)
  }

  sparkTest("A simple business pipeline written using SparkFunctions") {
    val customers = Sources.fromCsv(customerPath)
    val orders    = Sources.fromCsv(ordersPath)

    // I wrap this in a SparkFunction again because I need the SparkSession to import the implicits
    def countOrdersByCustomer(orders: DataFrame): SparkFunction[DataFrame] = SparkFunction { spark =>
      import spark.implicits._
      orders.groupBy('CUSTOMER_ID).agg(count('ID).as("COUNT"))
    }

    // No need for SparkSession, just a normal function
    def joinCustomersWithOrders(customers: DataFrame, ordersByCustomer: DataFrame): DataFrame = {
      customers
        .join(ordersByCustomer, ordersByCustomer("CUSTOMER_ID") === customers("ID"))
        .select(customers("FIRST_NAME"), customers("LAST_NAME"), ordersByCustomer("COUNT"))
    }

    val ordersByCustomer = orders.flatMap(countOrdersByCustomer)

    val result = (customers, ordersByCustomer).mapN(joinCustomersWithOrders).run(spark)

    val expected = Seq(("Bernard", "Chanson", 5L), ("Ron", "Swanson", 3L), ("Karl", "von Bauchspeck", 2L))
      .toDF("FIRST_NAME", "LAST_NAME", "COUNT")

    assertDatasetEquality(result, expected)
  }

  sparkTest("A simple pipeline to test functionality") {
    val persons = SparkFunction { spark: SparkSession =>
      import spark.implicits._
      Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()
    }

    val pipeline = persons
      .map(PersonTransformations.dedup)
      .flatMap(PersonTransformations.normalize)
      .map(PersonTransformations.returnBase)
      .makeSnapshot(OrcSink(File.temporaryFile().get().pathAsString))

    pipeline.run(spark)
  }

  sparkTest("A pipeline that combines multiple sources") {
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
      Seq(RawPerson("Karl von Bauchspeck", 28), RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35)).toDS(),
      orderedComparison = false
    )
  }

  sparkTest("A pipeline can write to multiple sinks at once") {
    val persons = SparkFunction { spark: SparkSession =>
      import spark.implicits._
      Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()
    }.map(PersonTransformations.dedup)
      .makeSnapshots(OrcSink("./target/output/orc"), ParquetSink("./target/output/parquet"))

    persons.run(spark)

    val expected =
      Seq(RawPerson("Bernard Chanson", 34), RawPerson("Ron Swanson", 35), RawPerson("Karl von Bauchspeck", 28)).toDS()

    assertDatasetEquality(spark.read.orc("./target/output/orc").as[RawPerson], expected, orderedComparison = false)
    assertDatasetEquality(spark.read.parquet("./target/output/parquet").as[RawPerson],
                          expected,
                          orderedComparison = false)
  }

  after {
    ("target" / "output").delete(swallowIOExceptions = true)
  }

  object PersonTransformations {

    def dedup(persons: Dataset[RawPerson]): Dataset[RawPerson] = persons.distinct()

    def normalize(persons: Dataset[RawPerson]) = SparkFunction { spark: SparkSession =>
      import spark.implicits._

      persons.map { raw =>
        val tokens = raw.name.split(" ")
        BasePerson(tokens(0), tokens(1), raw.age)
      }
    }

    def returnBase(basePersons: Dataset[BasePerson]): Dataset[BasePerson] = basePersons
  }
}
