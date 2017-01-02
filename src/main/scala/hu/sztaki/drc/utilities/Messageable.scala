package hu.sztaki.drc.utilities

import scala.reflect.ClassTag

trait Messageable {
  def send(message: Any): Unit
  def askWithRetry[T: ClassTag](message: Any): T
}
