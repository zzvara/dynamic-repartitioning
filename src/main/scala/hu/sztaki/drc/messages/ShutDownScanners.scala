package hu.sztaki.drc.messages

/**
  * Scan strategy message sent to workers.
  */
case class ShutDownScanners(stageID: Int)
  extends RepartitioningTrackerMessage
