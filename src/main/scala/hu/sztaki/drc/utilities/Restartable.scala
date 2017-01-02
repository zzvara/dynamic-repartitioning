package hu.sztaki.drc.utilities

trait Restartable extends Stoppable {

  // Nothing extra needed by default since doStop and doStart is needed anyways.
  protected def doRestart(): Unit = ()

  def restart(): this.type = {
    doStop()
    doStart()
    doRestart()
    this
  }
}

object Restartable {
  class Restarted(message: String) extends Exception(message)
}
