package be.dataminded.lighthouse.datalake

/**
  * A reference that allows you to fetch a [[DataLink]] from your environment
  */
case class DataUID(namespace: String, key: String)
