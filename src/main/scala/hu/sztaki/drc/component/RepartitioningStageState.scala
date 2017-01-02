package hu.sztaki.drc.component

import hu.sztaki.drc._
import hu.sztaki.drc.partitioner.Partitioner
import hu.sztaki.drc.utilities.Logger

import scala.collection.mutable

case class RepartitioningStageState[C <: Context[M],
                                    M <: Metrics[M]](
  var scanner: ScannerFactory[Scanner[C, M]],
  var scannedTasks: Option[mutable.Map[Long, WorkerTaskData[C, M]]] =
    Some(mutable.Map[Long, WorkerTaskData[C, M]]()),
  var partitioner: Option[Partitioner] = None,
  var version: Option[Int] = Some(0))
extends Logger {
  var _repartitioningFinished = false

  def isRepartitioningFinished: Boolean = _repartitioningFinished

  def finishRepartitioning(): Unit = {
    _repartitioningFinished = true
    scannedTasks = None
    version = None
  }
}