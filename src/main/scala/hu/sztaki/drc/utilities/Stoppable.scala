package hu.sztaki.drc.utilities

import hu.sztaki.drc.utilities.Startable.Started
import hu.sztaki.drc.utilities.Stoppable.Stopped

trait Stoppable extends Startable {
  private var _stopped = false

  def isStopped = _stopped

  protected def doStop(): Unit

  def stop(): this.type = {
    if (!isStarted) throw new Started("Can not stop that hasn't been started at all!")
    if (_stopped) throw new Stopped("Already stopped!")
    _stopped = true
    doStop()
    this
  }
}

object Stoppable {
  class Stopped(message: String) extends Exception(message)
}