package be.dataminded.lighthouse.demo

import be.dataminded.lighthouse.spark.SparkApplication
import org.apache.spark.sql.{DataFrame, Dataset}

// TODO: to be restructured once lighthouse interfaces are more clear

trait Environment {
  // Sets the config environment you would like to use
  def use(environment: String): Unit
  def dataRef(metadata: MetadataRef): DataRef
}

object Environment extends Environment {
  //TODO: Help mutability
  private[this] var env: String = "default"

  override def use(environment: String): Unit = {
    env = environment
  }

  override def dataRef(metadata: MetadataRef): DataRef = new FileDataRef
}

trait Metadata {
  val metadata: MetadataRef
}

// TODO: Trait or class
trait DataRef {
  def readDF(): DataFrame
  def readDS[T]: Dataset[T]
  def write[T](dataset: Dataset[T])
}

// TODO: define contructor params
class FileDataRef extends DataRef {
  override def readDF(): DataFrame                 = ???
  override def readDS[T]: Dataset[T]               = ???
  override def write[T](dataset: Dataset[T]): Unit = ???
}

// TODO: define contructor params
class JDBCDataRef extends DataRef {
  override def readDF(): DataFrame                 = ???
  override def readDS[T]: Dataset[T]               = ???
  override def write[T](dataset: Dataset[T]): Unit = ???
}

class MetadataRef(namespace: String, key: String) {}

// Data definitions
case class Audi(id: java.lang.Integer, brand: String, engine: String)
object Audi extends Metadata {
  val metadata = new MetadataRef(namespace = "topgear/cars", key = "Audi")
}

case class Volvo(id: java.lang.Integer, brand: String, engine: String)
object Volvo extends Metadata {
  val metadata = new MetadataRef(namespace = "topgear/cars", key = "Volvo")
}

object LighthouseDemo extends SparkApplication {
  import spark.implicits._
  Environment.use("production")
  // Get audi data ref
  val audiDataRef = Environment.dataRef(Audi.metadata)
  // convert audi to volvo
  val audiData: Dataset[Audi] = audiDataRef.readDS[Audi]
  // convert Audi to Volvo with some transformations
  val volvoData: Dataset[Volvo] = audiData.map(audi => Volvo(audi.id, audi.brand, audi.engine))
  // get VolvoDataRef
  val volvoDataRef = Environment.dataRef(Volvo.metadata)
  // write volvo data
  volvoDataRef.write(volvoData)

  println("Demo implementation")
}
