package be.dataminded.lighthouse.paramstore

import be.dataminded.lighthouse.common.FileSystem
import be.dataminded.lighthouse.datalake.LazyConfig
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Class helps retrieve secrets from typesafe [[Config]] file stored either on the local file system
  * or other supported storage backends.
  */
class FileParamStore(path: String, overrides: Map[String, String] = Map.empty) extends ParameterStore {

  private lazy val config: Config = {
    import scala.collection.JavaConverters._
    val defaultsConfig  = ConfigFactory.parseString(FileSystem.read(this.path))
    val overridesConfig = ConfigFactory.parseMap(overrides.asJava)
    overridesConfig.withFallback(defaultsConfig)
  }

  /**
    * Returns the lookup function to find a particular setting
    * @param key The key to retrieve
    */
  def lookup(key: String): LazyConfig[String] =
    LazyConfig {
      config.getString(key)
    }
}
