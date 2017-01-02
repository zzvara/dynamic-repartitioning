package hu.sztaki.drc.utilities

import com.typesafe.config.Config

object Factory {
  abstract class default[T]() extends Serializable {
    def apply(): T
  }
  abstract class withConfiguration[T]() extends Serializable {
    def apply(configuration: Config): T
  }
}
