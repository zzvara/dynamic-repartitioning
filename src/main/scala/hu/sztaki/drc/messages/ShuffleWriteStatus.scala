package hu.sztaki.drc.messages

import hu.sztaki.drc.Sampling

import scala.reflect.ClassTag

/**
  * Shuffle write status message.
  */
case class ShuffleWriteStatus[T: ClassTag](
  stageID: Int,
  taskID: Long,
  partitionID: Int,
  keyHistogram: Sampling) extends RepartitioningTrackerMessage
