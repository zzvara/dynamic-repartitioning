package hu.sztaki.drc.utilities

import scala.reflect.ClassTag

trait Messageable {
  def send(message: Any): Unit
  def askSync[T: ClassTag](message: Any): T
}
