package be.dataminded.lighthouse.datalake

import scala.collection.mutable

object Datalake {
  val PropertyName       = "lighthouse.environment"
  val DefaultEnvironment = "test"
}

/**
  * Interface containing all the configuration for a given environment
  */
trait Datalake {

  import Datalake._

  type Environment        = Map[DataUID, DataLink]
  type EnvironmentBuilder = mutable.MapBuilder[DataUID, DataLink, Environment]

  private var enabledEnvironment: Environment = _

  def currentEnvironment: Environment = enabledEnvironment

  /**
    * The current environment that is enabled.
    */
  val environmentName: String = {
    Option(System.getProperty(PropertyName)).getOrElse(DefaultEnvironment)
  }

  def apply(dataUID: DataUID): DataLink = getDataLink(dataUID)

  /**
    * Retrieve a [[DataLink]] from the [[Datalake]] object. If there is no [[DataLink]] configured with the given
    * [[DataUID]] a [[NoSuchElementException]] will be thrown
    *
    * @param dataUID The [[DataUID]]-identifier for which to retrieve a `DataLink`
    * @return The DataLink that matches the given [[DataUID]]
    */
  def getDataLink(dataUID: DataUID): DataLink = enabledEnvironment(dataUID)

  protected def environment(name: String)(f: EnvironmentBuilder => EnvironmentBuilder): Unit =
    name match {
      case envName if envName == environmentName => enabledEnvironment = f(newEmptyEnvironment()).result()
      case _                                     =>
    }

  private def newEmptyEnvironment() = new mutable.MapBuilder[DataUID, DataLink, Environment](Map.empty)
}
