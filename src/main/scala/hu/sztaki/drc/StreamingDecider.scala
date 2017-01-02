package hu.sztaki.drc

abstract class StreamingDecider[Stream](
  streamID: Int,
  stream: Stream,
  val perBatchSamplingRate: Int = 1,
  resourceStateHandler: Option[() => Int] = None)
  extends Decider(streamID, resourceStateHandler){
  def onPartitionMetricsArrival(partitionID: Int, recordsRead: Long): Unit
}
