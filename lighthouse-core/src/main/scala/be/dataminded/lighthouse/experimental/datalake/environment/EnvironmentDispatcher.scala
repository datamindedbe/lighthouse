package be.dataminded.lighthouse.experimental.datalake.environment
import be.dataminded.lighthouse.datalake.Datalake.{DefaultEnvironment, PropertyName}

trait EnvironmentDispatcher[E <: Environment] {
  private  val environmentName: String =
    Option(System.getProperty(PropertyName)).getOrElse(DefaultEnvironment)

  def enabledEnvironments: Set[E]

  def currentEnvironment: E =
    enabledEnvironments.find(_.name == environmentName)
      .getOrElse(throw new IllegalArgumentException(s"DataEnvironment not valid: $environmentName"))
}
