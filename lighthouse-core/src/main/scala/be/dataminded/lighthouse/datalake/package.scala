package be.dataminded.lighthouse

import java.util.Properties

import scala.language.implicitConversions

package object datalake {

  type LazyConfig[T] = () => T

  object LazyConfig {
    def apply[T](body: => T): LazyConfig[T] = () => body()
  }

  implicit def asLazyConfig[T](value: T): LazyConfig[T] = () => value

  implicit def asProperties(map: Map[String, String]): Properties =
    map.foldLeft(new Properties()) { case (props, (k, v)) => props.setProperty(k, v); props }
}
