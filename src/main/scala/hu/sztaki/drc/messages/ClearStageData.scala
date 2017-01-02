package hu.sztaki.drc.messages

/**
  * Scan strategy message sent to workers.
  */
case class ClearStageData(stageID: Int) extends RepartitioningTrackerMessage
