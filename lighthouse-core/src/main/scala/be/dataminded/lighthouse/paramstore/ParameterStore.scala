package be.dataminded.lighthouse.paramstore

import be.dataminded.lighthouse.datalake.LazyConfig

trait ParameterStore {
  def lookup(key: String): LazyConfig[String]
}
