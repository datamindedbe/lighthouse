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

  type Environment        = Map[DataUID, DataLink]
  type EnvironmentBuilder = mutable.MapBuilder[DataUID, DataLink, Environment]

  private var environments: Map[String, EnvironmentBuilder => EnvironmentBuilder] = Map.empty

  private lazy val currentEnvironment: Environment = {
    environments(environmentName)
      .apply(new mutable.MapBuilder[DataUID, DataLink, Environment](Map.empty))
      .result()
  }

  lazy val environmentName: String = {
    Option(System.getProperty(Datalake.PropertyName)).getOrElse(Datalake.DefaultEnvironment)
  }

  def apply(dataUID: DataUID): DataLink = getDataLink(dataUID)

  def getDataLink(dataUID: DataUID): DataLink = currentEnvironment(dataUID)

  protected def environment(name: String)(f: (EnvironmentBuilder) => EnvironmentBuilder): Unit = {
    environments = environments.updated(name, f)
  }
}
