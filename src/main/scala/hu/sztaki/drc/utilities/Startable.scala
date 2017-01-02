package hu.sztaki.drc.utilities

trait Startable extends Logger {
  private var _started = false

  def isStarted = _started

  protected def doStart(): Unit

  def start(): this.type = {
    if (_started) {
      logError("Already started!")
    } else {
      _started = true
      doStart()
    }
    this
  }
}

object Startable {
  class Started(message: String) extends Exception(message)
}
