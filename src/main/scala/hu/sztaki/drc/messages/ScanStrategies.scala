package hu.sztaki.drc.messages

case class ScanStrategies(scanStrategies: List[ScanStrategy])
  extends RepartitioningTrackerMessage
