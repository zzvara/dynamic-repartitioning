package hu.sztaki.drc

import hu.sztaki.drc.utilities.Configuration

abstract class Throughput[
  TaskContext <: Context[TaskMetrics],
  TaskMetrics <: Metrics[TaskMetrics]](
  totalSlots: Int,
  histogramDrop: (Int, Long, Int, Sampling) => Unit)
extends Scanner[TaskContext, TaskMetrics](totalSlots, histogramDrop) {
  private var lastHistogramHeight: Long = 0

  /**
    * Reconfigures the data-characteristics sampling by updating the total slots available.
    * @param dataCharacteristics Data characteristics shuffle write metrics.
    */
  def updateTotalSlots(dataCharacteristics: Sampling): Unit = {
    /**
      * @todo Why is this commented out?
      */
    // dataCharacteristics.updateTotalSlots(totalSlots)
  }

  def whenTaskCompleted(context: TaskContext): Unit = {
    logDebug(s"Detected completion for stage [${taskContext.stageID()}] task " +
             s"[${taskContext.attemptNumber()}]. Shutting down [Throughput] scanner.")
    isRunning = false
  }

  /**
    * Drop in point.
    */
  def whenStarted(): Unit = { }

  override def run(): Unit = {
    logDebug(s"Running scanner for stage [${taskContext.stageID()}] task" +
             s" [${taskContext.attemptNumber()}].")
    require(taskContext != null, "Scanner needs to have a valid task context!")
    isRunning = true

    updateTotalSlots(taskContext.metrics().writeCharacteristics)
    whenStarted()

    val interval =
      Configuration.internal().getInt("spark.repartitioning.throughput.interval")
    val recordBound =
      Configuration.internal().getInt("spark.repartitioning.throughput.record-bound")

    Thread.sleep(interval)
    while (isRunning) {
      val dataCharacteristics = taskContext.metrics().writeCharacteristics
      val histogramHeightDelta = dataCharacteristics.recordsPassed - lastHistogramHeight
      if (dataCharacteristics.width == 0) {
        logInfo(s"Histogram is empty for task [${taskContext.attemptNumber()}]. " +
                s"Doing Nothing.")
      } else if (recordBound > histogramHeightDelta) {
        logInfo(s"Not enough records [$histogramHeightDelta] " +
                s"processed to send the histogram to the driver.")
      } else {
        lastHistogramHeight = dataCharacteristics.recordsPassed
        histogramDrop(
          taskContext.stageID(),
          taskContext.attemptNumber(),
          taskContext.partitionID(),
          dataCharacteristics)
      }
      Thread.sleep(interval)
      updateTotalSlots(taskContext.metrics().writeCharacteristics)
    }
  }
}
