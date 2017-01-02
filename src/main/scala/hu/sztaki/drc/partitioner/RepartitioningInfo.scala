package hu.sztaki.drc.partitioner

import hu.sztaki.drc.{Sampler, Metrics}
import hu.sztaki.drc.utilities.Logger

class RepartitioningInfo[TaskMetrics <: Metrics[TaskMetrics]](
  val stageID: Int,
  val taskID: Long,
  val executorName: String,
  val taskMetrics: TaskMetrics,
  var repartitioner: Option[Partitioner] = None,
  var version: Option[Int] = Some(0)) extends Serializable with Logger {

  var trackingFinished = false

  logInfo(s"Created for stage [$stageID], task [$taskID]. " +
          s"Repartitioner is [$repartitioner] with version [$version].")

  def updateRepartitioner(repartitioner: Partitioner, version: Int): Unit = {
    this.repartitioner = Some(repartitioner)
    this.version = Some(version)
    logInfo(s"Updating for stage [$stageID], task [$taskID]. " +
      s"Repartitioner is [$repartitioner] with new version [$version].")
  }

  def getHistogramMeta: Sampler = {
    taskMetrics.writeCharacteristics
  }

  def finishTracking(): Unit = {
    trackingFinished = true
    logInfo(s"Finished tracking for stage [$stageID], task [$taskID]. " +
            s"Repartitioner is [$repartitioner] with new version [$version].")
  }
}