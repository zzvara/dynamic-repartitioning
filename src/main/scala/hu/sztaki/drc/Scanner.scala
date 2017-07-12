package hu.sztaki.drc

import hu.sztaki.drc.utilities.Logger

/**
  * Decides when to send the histogram to the master from the workers.
  *
  * This strategy should run somewhere near the TaskMetrics and should decide
  * based on many factors when to send the histogram to the master.
  * Also, it should declare the sampling method.
  */
abstract class Scanner[
  TaskContext <: Context[TaskMetrics],
  TaskMetrics <: Metrics[TaskMetrics]](
  val totalSlots: Int,
  histogramDrop: (Int, Long, Int, Sampling) => Unit)
extends Serializable with Runnable with Logger {
  var taskContext: TaskContext = _

  def setContext(context: TaskContext): Unit = {
    taskContext = context
  }

  var isRunning: Boolean = false

  def stop(): Unit = {
    isRunning = false
  }
}
