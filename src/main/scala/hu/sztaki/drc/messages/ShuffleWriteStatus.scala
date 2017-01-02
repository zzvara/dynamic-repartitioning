package hu.sztaki.drc.messages

import hu.sztaki.drc.Sampler

import scala.reflect.ClassTag

/**
  * Shuffle write status message.
  */
case class ShuffleWriteStatus[T: ClassTag](
  stageID: Int,
  taskID: Long,
  partitionID: Int,
  keyHistogram: Sampler) extends RepartitioningTrackerMessage
