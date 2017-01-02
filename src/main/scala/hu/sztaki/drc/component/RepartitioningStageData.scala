package hu.sztaki.drc.component

import hu.sztaki.drc._
import hu.sztaki.drc.partitioner.Partitioner

import scala.collection.mutable

case class RepartitioningStageData[TaskContext <: TaskContextInterface[TaskMetrics],
                                   TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  var scanner: ScannerFactory[Scanner[TaskContext, TaskMetrics]],
  var scannedTasks: Option[mutable.Map[Long, WorkerTaskData[TaskContext, TaskMetrics]]] =
    Some(mutable.Map[Long, WorkerTaskData[TaskContext, TaskMetrics]]()),
  var partitioner: Option[Partitioner] = None,
  var version: Option[Int] = Some(0)) {

  var _repartitioningFinished = false

  def isRepartitioningFinished: Boolean = _repartitioningFinished

  def finishRepartitioning(): Unit = {
    _repartitioningFinished = true
    scannedTasks = None
    version = None
  }
}