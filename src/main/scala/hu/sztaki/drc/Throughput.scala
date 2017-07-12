package hu.sztaki.drc

import hu.sztaki.drc.utilities.Configuration

abstract class Throughput[
  TaskContext <: Context[TaskMetrics],
  TaskMetrics <: Metrics[TaskMetrics]](
  totalSlots: Int,
  histogramDrop: (Int, Long, Int, Sampling) => Unit)
extends Scanner[TaskContext, TaskMetrics](totalSlots, histogramDrop) {
  private var lastHistogramHeight: Long = 0
  private val keyHistogramWidth: Int =
    Configuration.internal().getInt("spark.repartitioning.key-histogram.truncate")

  /**
    * Reconfigures the data-characteristics sampling by updating the total slots available.
    * @param dataCharacteristics Data characteristics shuffle write metrics.
    */
  def updateTotalSlots(dataCharacteristics: Sampling): Unit = {
    logInfo("Updating number of total slots.")
    // dataCharacteristics.updateTotalSlots(totalSlots)
  }

  def whenTaskCompleted(context: TaskContext): Unit = {
    logInfo(s"Detected completion for stage ${taskContext.stageID()} task" +
      s" ${taskContext.attemptNumber()}.")
    isRunning = false
  }

  def whenStarted(): Unit = {}

  override def run(): Unit = {
    logInfo(s"Running scanner for stage ${taskContext.stageID()} task" +
      s" ${taskContext.attemptNumber()}.")
    require(taskContext != null, "Scanner needs to have a valid task context!")
    isRunning = true
    whenStarted()

    updateTotalSlots(taskContext.metrics().writeCharacteristics)

    Thread.sleep(Configuration.internal().getInt("spark.repartitioning.throughput.interval"))
    while (isRunning) {
      val dataCharacteristics = taskContext.metrics().writeCharacteristics
      val recordBound =
        Configuration.internal().getInt("spark.repartitioning.throughput.record-bound")
      val histogramHeightDelta = dataCharacteristics.recordsPassed - lastHistogramHeight
      if (dataCharacteristics.width == 0) {
        logInfo(s"Histogram is empty for task ${taskContext.attemptNumber()}. " +
                s"Doing Nothing.")
      } else if (recordBound > histogramHeightDelta) {
        logInfo(s"Not enough records ($histogramHeightDelta) " +
                s"processed to send the histogram to the driver.")
      } else {
        lastHistogramHeight = dataCharacteristics.recordsPassed
        histogramDrop(
          taskContext.stageID(),
          taskContext.attemptNumber(),
          taskContext.partitionID(),
          dataCharacteristics)
      }
      Thread.sleep(Configuration.internal().getInt("spark.repartitioning.throughput.interval"))
      updateTotalSlots(taskContext.metrics().writeCharacteristics)
    }
    logInfo(s"Scanner is finishing for stage ${taskContext.stageID()} task" +
            s" ${taskContext.attemptNumber()}.")
  }
}
