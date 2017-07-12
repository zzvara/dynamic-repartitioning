package hu.sztaki.drc

import hu.sztaki.drc.partitioner.RepartitioningInfo

abstract class Metrics[M <: Metrics[M]] {
  var repartitioningInfo: Option[RepartitioningInfo[M]]
  def writeCharacteristics: Sampling
}
