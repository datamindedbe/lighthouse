package be.dataminded.lighthouse

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import scala.language.implicitConversions

package object datalake {

  type LazyConfig[T] = () => T

  object LazyConfig {
    def apply[T](body: => T): LazyConfig[T] = () => body()
  }

  implicit def asLazyConfig[T](value: T): LazyConfig[T] = () => value

  implicit def asOriginal[T](lazyValue: LazyConfig[T]): T = lazyValue()

  implicit def asProperties(map: Map[String, String]): Properties =
    map.foldLeft(new Properties()) { case (props, (k, v)) => props.setProperty(k, v); props }

  implicit def tupleAsDataUID(tuple: (String, String)): DataUID = {
    val (namespace, key) = tuple
    DataUID(namespace, key)
  }

  implicit def localDateAsStringPartition(date: LocalDate): LazyConfig[String] = {
    s"year=${date.format(DateTimeFormatter.ofPattern("yyyy"))}" +
      s"/month=${date.format(DateTimeFormatter.ofPattern("MM"))}" +
      s"/day=${date.format(DateTimeFormatter.ofPattern("dd"))}"
  }
}
