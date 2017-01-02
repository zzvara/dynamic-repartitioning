package hu.sztaki.drc

import hu.sztaki.drc.partitioner.RepartitioningInfo

abstract class TaskMetricsInterface[TaskMetrics <: TaskMetricsInterface[TaskMetrics]] {
  var repartitioningInfo: Option[RepartitioningInfo[TaskMetrics]]
  def writeCharacteristics: Sampler
}
