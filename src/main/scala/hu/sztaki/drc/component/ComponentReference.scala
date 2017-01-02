package hu.sztaki.drc.component

abstract class ComponentReference {
  def send(message: Any): Unit
}
