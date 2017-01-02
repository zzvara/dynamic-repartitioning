package hu.sztaki.drc.component

abstract class Reference {
  def send(message: Any): Unit
  def !(message: Any): Unit
}
