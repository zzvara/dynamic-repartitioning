package hu.sztaki.drc.messages

import hu.sztaki.drc.partitioner.Partitioner

/**
  * Repartitioning strategy message sent to workers.
  */
case class RepartitioningStrategy(
  stageID: Int,
  repartitioner: Partitioner,
  version: Int) extends RepartitioningTrackerMessage
